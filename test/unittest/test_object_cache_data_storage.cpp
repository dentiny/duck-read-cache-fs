#include "catch/catch.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <utility>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "in_mem_cache_block.hpp"
#include "object_cache_data_cache_storage.hpp"
#include "page_aligned_data_chunk.hpp"
#include "thread_pool.hpp"

using namespace duckdb; // NOLINT

namespace {

// Allocate a chunk with deterministic content for content-equality assertions.
PageAlignedDataChunk MakeFilledChunk(idx_t size, char fill) {
	auto chunk = AllocatePageAlignedChunk(size);
	for (idx_t i = 0; i < size; ++i) {
		chunk.data()[i] = fill;
	}
	chunk.length = size;
	return chunk;
}

void PutBlock(ObjectCacheStorage &storage, const string &fname, idx_t start_off, idx_t blk_size,
              const string &version_tag, char fill = 'x') {
	storage.Put(InMemCacheBlock {fname, start_off, blk_size}, MakeFilledChunk(blk_size, fill), version_tag);
}

bool AllBytesEqual(const PageAlignedDataChunk &chunk, char expected) {
	const char *data = chunk.data();
	for (idx_t i = 0; i < chunk.length; ++i) {
		if (data[i] != expected) {
			return false;
		}
	}
	return true;
}

} // namespace

TEST_CASE("ObjectCacheStorage Put/Get roundtrip", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	const InMemCacheBlock key {"/foo", 0, 1024};
	storage->Put(key, MakeFilledChunk(1024, 'a'), /*version_tag=*/"v1");

	auto pinned = storage->Get(key, /*expected_version_tag=*/"v1");
	REQUIRE(pinned);
	REQUIRE(pinned->Data().length == 1024);
	REQUIRE(AllBytesEqual(pinned->Data(), 'a'));

	REQUIRE(storage->Keys().size() == 1);

	REQUIRE(storage->Get(key, /*expected_version_tag=*/""));
}

TEST_CASE("ObjectCacheStorage Get miss on unknown key", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);
	const InMemCacheBlock key {"/foo", 0, 1024};
	REQUIRE_FALSE(storage->Get(key, /*expected_version_tag=*/""));
	REQUIRE(storage->Keys().empty());
}

TEST_CASE("ObjectCacheStorage Get with version mismatch evicts and returns nullopt", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	const InMemCacheBlock key {"/foo", 0, 1024};
	storage->Put(key, MakeFilledChunk(1024, 'a'), /*version_tag=*/"v1");
	REQUIRE(storage->Keys().size() == 1);

	REQUIRE_FALSE(storage->Get(key, /*expected_version_tag=*/"v2"));
	REQUIRE(storage->Keys().empty());
}

TEST_CASE("ObjectCacheStorage destructor-driven map sync on ObjectCache eviction", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	PutBlock(*storage, "/foo", 0, 1024, /*version_tag=*/"v1");
	PutBlock(*storage, "/foo", 1024, 1024, /*version_tag=*/"v1");
	REQUIRE(storage->Keys().size() == 2);

	auto &obj_cache = db.instance->GetObjectCache();
	const idx_t evicted = obj_cache.EvictToReduceMemory(obj_cache.GetCurrentMemory());
	REQUIRE(evicted > 0);
	REQUIRE(storage->Keys().empty());
}

TEST_CASE("ObjectCacheStorage timeout invalidates on Get", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/5);

	PutBlock(*storage, "/foo", 0, 1024, /*version_tag=*/"v1");
	REQUIRE(storage->Keys().size() == 1);

	// To reduce flakiness, sleep for 10x timeout latency.
	std::this_thread::sleep_for(std::chrono::milliseconds(50));

	REQUIRE_FALSE(storage->Get(InMemCacheBlock {"/foo", 0, 1024}, /*expected_version_tag=*/""));
	REQUIRE(storage->Keys().empty());
}

TEST_CASE("ObjectCacheStorage Clear drains map and ObjectCache", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	constexpr idx_t N = 16;
	for (idx_t idx = 0; idx < N; ++idx) {
		PutBlock(*storage, "/foo", idx * 1024, 1024, /*version_tag=*/"v1");
	}
	REQUIRE(storage->Keys().size() == N);
	const idx_t bytes_before = db.instance->GetObjectCache().GetCurrentMemory();
	REQUIRE(bytes_before >= N * 1024);

	storage->Clear();

	REQUIRE(storage->Keys().empty());
	REQUIRE(db.instance->GetObjectCache().GetCurrentMemory() == 0);
}

TEST_CASE("ObjectCacheStorage prefix-scoped Clear for one file", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	PutBlock(*storage, "/a", 0, 512, /*version_tag=*/"v1");
	PutBlock(*storage, "/a", 512, 512, /*version_tag=*/"v1");
	PutBlock(*storage, "/b", 0, 512, /*version_tag=*/"v1");
	REQUIRE(storage->Keys().size() == 3);

	const InMemCacheBlock start {"/a", 0, 0};
	storage->Clear(start, [](const InMemCacheBlock &k) { return k.fname == "/a"; });

	auto remaining = storage->Keys();
	REQUIRE(remaining.size() == 1);
	REQUIRE(remaining[0].fname == "/b");
	REQUIRE_FALSE(storage->Get(InMemCacheBlock {"/a", 0, 512}, /*expected_version_tag=*/""));
	REQUIRE_FALSE(storage->Get(InMemCacheBlock {"/a", 512, 512}, /*expected_version_tag=*/""));
	REQUIRE(storage->Get(InMemCacheBlock {"/b", 0, 512}, /*expected_version_tag=*/""));
}

TEST_CASE("ObjectCacheStorage replace-on-duplicate-Put keeps single map entry with latest meta",
          "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);
	auto &obj_cache = db.instance->GetObjectCache();

	const InMemCacheBlock key {"/x", 0, 512};
	storage->Put(key, MakeFilledChunk(512, '1'), /*version_tag=*/"v1");
	REQUIRE(obj_cache.GetCurrentMemory() == 512);

	storage->Put(key, MakeFilledChunk(512, '2'), /*version_tag=*/"v2");
	REQUIRE(storage->Keys().size() == 1);
	// Second Put displaced v1 in ObjectCache; total reservation is still one block, not two.
	REQUIRE(obj_cache.GetCurrentMemory() == 512);

	{
		auto pinned = storage->Get(key, /*expected_version_tag=*/"v2");
		REQUIRE(pinned);
		REQUIRE(AllBytesEqual(pinned->Data(), '2'));
	}
	REQUIRE_FALSE(storage->Get(key, /*expected_version_tag=*/"v1"));
	REQUIRE(storage->Keys().empty());
	REQUIRE(obj_cache.GetCurrentMemory() == 0);
}

TEST_CASE("ObjectCacheStorage Delete removes both map and ObjectCache entry", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	const InMemCacheBlock key {"/foo", 0, 1024};
	PutBlock(*storage, "/foo", 0, 1024, /*version_tag=*/"v1");
	REQUIRE(storage->Delete(key));
	REQUIRE_FALSE(storage->Delete(key));
	REQUIRE(storage->Keys().empty());
}

TEST_CASE("ObjectCacheStorage Take drains storage and yields owning chunks", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	PutBlock(*storage, "/foo", 0, 1024, /*version_tag=*/"v1", /*fill=*/'a');
	PutBlock(*storage, "/foo", 1024, 1024, /*version_tag=*/"v1", /*fill=*/'b');

	auto taken = storage->Take();
	REQUIRE(taken.size() == 2);
	for (auto &kv : taken) {
		REQUIRE(kv.second != nullptr);
		REQUIRE(kv.second->data.length == 1024);
		REQUIRE(kv.second->version_tag == "v1");
		const char expected_fill = kv.first.start_off == 0 ? 'a' : 'b';
		REQUIRE(AllBytesEqual(kv.second->data, expected_fill));
	}
	REQUIRE(storage->Keys().empty());
	REQUIRE(db.instance->GetObjectCache().GetCurrentMemory() == 0);
}

TEST_CASE("ObjectCacheStorage concurrent Put and Get", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	constexpr idx_t THREAD_COUNT = 8;
	constexpr idx_t PER_THREAD = 32;
	constexpr idx_t BLK = 256;

	std::atomic<idx_t> hits {0};
	ThreadPool tp(THREAD_COUNT);
	std::vector<std::future<void>> futures;
	futures.reserve(THREAD_COUNT);
	for (idx_t t = 0; t < THREAD_COUNT; ++t) {
		futures.emplace_back(tp.Push([&, t]() {
			const char fill = static_cast<char>('A' + (t % 26));
			for (idx_t i = 0; i < PER_THREAD; ++i) {
				const idx_t off = (t * PER_THREAD + i) * BLK;
				InMemCacheBlock key {"/concurrent", off, BLK};
				storage->Put(key, MakeFilledChunk(BLK, fill), /*version_tag=*/"v");
				auto pinned = storage->Get(key, /*expected_version_tag=*/"v");
				if (pinned) {
					REQUIRE(pinned->Data().length == BLK);
					REQUIRE(AllBytesEqual(pinned->Data(), fill));
					hits.fetch_add(1);
				}
			}
		}));
	}
	tp.Wait();
	for (auto &fut : futures) {
		fut.get();
	}

	REQUIRE(hits.load() == THREAD_COUNT * PER_THREAD);
	REQUIRE(storage->Keys().size() == THREAD_COUNT * PER_THREAD);
}

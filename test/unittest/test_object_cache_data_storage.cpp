// Storage-level unit tests for `ObjectCacheStorage`. No reader involvement.
//
// Covers the design's required scenarios:
//   - destructor-driven map sync on global LRU eviction
//   - timeout-based lazy expiry on Get
//   - clear-on-switch (Clear() drains both our map and ObjectCache)
//   - prefix-scoped Clear(start_key, filter) for file-level invalidation
//   - replace-on-duplicate-Put preserves a single map entry with the latest version_tag
//   - concurrent Put/Get does not race (TSan-clean)

#include "catch/catch.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <utility>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "in_mem_cache_block.hpp"
#include "object_cache_data_cache_storage.hpp"
#include "page_aligned_data_chunk.hpp"

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

} // namespace

TEST_CASE("ObjectCacheStorage Put/Get roundtrip", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	const InMemCacheBlock key {"/foo", 0, 1024};
	storage->Put(key, MakeFilledChunk(1024, 'a'), /*version_tag=*/"v1");

	auto pinned = storage->Get(key, /*expected_version_tag=*/"v1");
	REQUIRE(pinned);
	REQUIRE(pinned->Data().length == 1024);
	REQUIRE(pinned->Data().data()[0] == 'a');
	REQUIRE(pinned->Data().data()[1023] == 'a');

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

	// Force eviction by asking ObjectCache to free more bytes than we hold; this drops every entry
	// whose shared_ptr we don't pin. Destructors fire on this thread => our map drains in lock-step.
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

	std::this_thread::sleep_for(std::chrono::milliseconds(50));

	// Stale entry => Get returns nullopt and triggers an ObjectCache::Delete; destructor finalises map.
	REQUIRE_FALSE(storage->Get(InMemCacheBlock {"/foo", 0, 1024}, /*expected_version_tag=*/""));
	REQUIRE(storage->Keys().empty());
}

TEST_CASE("ObjectCacheStorage Clear drains map and ObjectCache", "[object cache storage]") {
	DuckDB db;
	auto storage = make_shared_ptr<ObjectCacheStorage>(*db.instance, /*timeout_millisec=*/0);

	constexpr idx_t N = 16;
	for (idx_t i = 0; i < N; ++i) {
		PutBlock(*storage, "/foo", i * 1024, 1024, /*version_tag=*/"v1");
	}
	REQUIRE(storage->Keys().size() == N);
	const idx_t bytes_before = db.instance->GetObjectCache().GetCurrentMemory();
	REQUIRE(bytes_before >= N * 1024);

	storage->Clear();

	REQUIRE(storage->Keys().empty());
	// Every block we owned has been dropped from ObjectCache; its accounting is back to whatever
	// was there before (which may not be zero because the CacheHttpfsInstanceState entry itself
	// is stored in ObjectCache, but it is non-evictable and unaccounted).
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

	const InMemCacheBlock key {"/x", 0, 512};
	storage->Put(key, MakeFilledChunk(512, '1'), /*version_tag=*/"v1");
	storage->Put(key, MakeFilledChunk(512, '2'), /*version_tag=*/"v2");

	REQUIRE(storage->Keys().size() == 1);
	auto pinned = storage->Get(key, /*expected_version_tag=*/"v2");
	REQUIRE(pinned);
	REQUIRE(pinned->Data().data()[0] == '2');
	REQUIRE_FALSE(storage->Get(key, /*expected_version_tag=*/"v1"));
	REQUIRE(storage->Keys().empty());
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

	std::vector<std::thread> threads;
	threads.reserve(THREAD_COUNT);
	std::atomic<idx_t> hits {0};
	for (idx_t t = 0; t < THREAD_COUNT; ++t) {
		threads.emplace_back([&, t]() {
			for (idx_t i = 0; i < PER_THREAD; ++i) {
				const idx_t off = (t * PER_THREAD + i) * BLK;
				InMemCacheBlock key {"/concurrent", off, BLK};
				storage->Put(key, MakeFilledChunk(BLK, static_cast<char>('A' + (t % 26))), /*version_tag=*/"v");
				auto pinned = storage->Get(key, /*expected_version_tag=*/"v");
				if (pinned) {
					REQUIRE(pinned->Data().length == BLK);
					hits.fetch_add(1, std::memory_order_relaxed);
				}
			}
		});
	}
	for (auto &th : threads) {
		th.join();
	}

	// Each key is unique to its thread; every Get should hit unless ObjectCache LRU evicted some
	// entries. With 8 * 32 * 256 = 64KiB total, there's no chance of ObjectCache eviction
	// (default 8 GiB cap).
	REQUIRE(hits.load() == THREAD_COUNT * PER_THREAD);
	REQUIRE(storage->Keys().size() == THREAD_COUNT * PER_THREAD);
}

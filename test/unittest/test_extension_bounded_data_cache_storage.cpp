#include "catch/catch.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

#include "duckdb/common/helper.hpp"
#include "duckdb/common/string.hpp"
#include "extension_bounded_data_cache_storage.hpp"
#include "in_mem_cache_block.hpp"
#include "lru_eviction_policy.hpp"
#include "page_aligned_data_chunk.hpp"

using namespace duckdb; // NOLINT

namespace {

constexpr idx_t TEST_BLOCK_SIZE = 16;

PageAlignedDataChunk MakeFilledChunk(char fill) {
	auto chunk = AllocatePageAlignedChunk(TEST_BLOCK_SIZE);
	std::fill(chunk.data(), chunk.data() + TEST_BLOCK_SIZE, fill);
	chunk.length = TEST_BLOCK_SIZE;
	return chunk;
}

InMemCacheBlock MakeKey(const string &fname, idx_t idx) {
	return InMemCacheBlock {fname, idx * TEST_BLOCK_SIZE, TEST_BLOCK_SIZE};
}

unique_ptr<ExtensionBoundedDataCacheStorage> MakeLruStorage(size_t max_entries, uint64_t timeout_millisec = 0) {
	return make_uniq<ExtensionBoundedDataCacheStorage>(
	    max_entries, timeout_millisec, make_uniq<LruEvictionPolicy<InMemCacheBlock, InMemCacheBlockLess>>());
}

} // namespace

TEST_CASE("ExtensionBoundedDataCacheStorage put and get", "[extension bounded storage test]") {
	auto storage = MakeLruStorage(/*max_entries=*/2);
	REQUIRE_FALSE(storage->Get(MakeKey("f", 0), /*expected_version_tag=*/""));

	storage->Put(MakeKey("f", 0), MakeFilledChunk('a'), /*version_tag=*/"v1");
	auto pinned = storage->Get(MakeKey("f", 0), /*expected_version_tag=*/"v1");
	REQUIRE(pinned);
	REQUIRE(pinned->Data().data()[0] == 'a');

	// Replace in place.
	storage->Put(MakeKey("f", 0), MakeFilledChunk('b'), /*version_tag=*/"v1");
	pinned = storage->Get(MakeKey("f", 0), /*expected_version_tag=*/"");
	REQUIRE(pinned);
	REQUIRE(pinned->Data().data()[0] == 'b');
	REQUIRE(storage->Keys().size() == 1);

	// Version mismatch invalidates the entry.
	REQUIRE_FALSE(storage->Get(MakeKey("f", 0), /*expected_version_tag=*/"v2"));
	REQUIRE(storage->Keys().empty());
}

TEST_CASE("ExtensionBoundedDataCacheStorage evicts with policy", "[extension bounded storage test]") {
	auto storage = MakeLruStorage(/*max_entries=*/2);
	storage->Put(MakeKey("f", 0), MakeFilledChunk('a'), /*version_tag=*/"");
	storage->Put(MakeKey("f", 1), MakeFilledChunk('b'), /*version_tag=*/"");

	// Refresh block 0, so block 1 is evicted on the next insertion.
	REQUIRE(storage->Get(MakeKey("f", 0), /*expected_version_tag=*/""));
	storage->Put(MakeKey("f", 2), MakeFilledChunk('c'), /*version_tag=*/"");
	REQUIRE(storage->Get(MakeKey("f", 0), /*expected_version_tag=*/""));
	REQUIRE_FALSE(storage->Get(MakeKey("f", 1), /*expected_version_tag=*/""));
	REQUIRE(storage->Get(MakeKey("f", 2), /*expected_version_tag=*/""));

	// Deleted entries free capacity in the policy as well.
	REQUIRE(storage->Delete(MakeKey("f", 0)));
	REQUIRE_FALSE(storage->Delete(MakeKey("f", 0)));
	storage->Put(MakeKey("f", 3), MakeFilledChunk('d'), /*version_tag=*/"");
	REQUIRE(storage->Get(MakeKey("f", 2), /*expected_version_tag=*/""));
	REQUIRE(storage->Get(MakeKey("f", 3), /*expected_version_tag=*/""));
}

TEST_CASE("ExtensionBoundedDataCacheStorage timeout", "[extension bounded storage test]") {
	auto storage = MakeLruStorage(/*max_entries=*/2, /*timeout_millisec=*/50);
	storage->Put(MakeKey("f", 0), MakeFilledChunk('a'), /*version_tag=*/"");
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	REQUIRE_FALSE(storage->Get(MakeKey("f", 0), /*expected_version_tag=*/""));
	REQUIRE(storage->Keys().empty());

	// Expired entries are dropped by [Take].
	storage->Put(MakeKey("f", 1), MakeFilledChunk('b'), /*version_tag=*/"");
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	storage->Put(MakeKey("f", 2), MakeFilledChunk('c'), /*version_tag=*/"");
	auto taken = storage->Take();
	REQUIRE(taken.size() == 1);
	REQUIRE(taken[0].first.start_off == 2 * TEST_BLOCK_SIZE);
	REQUIRE(storage->Keys().empty());
}

TEST_CASE("ExtensionBoundedDataCacheStorage clear by file", "[extension bounded storage test]") {
	auto storage = MakeLruStorage(/*max_entries=*/4);
	storage->Put(MakeKey("a", 0), MakeFilledChunk('a'), /*version_tag=*/"");
	storage->Put(MakeKey("b", 0), MakeFilledChunk('b'), /*version_tag=*/"");
	storage->Put(MakeKey("b", 1), MakeFilledChunk('b'), /*version_tag=*/"");
	storage->Put(MakeKey("c", 0), MakeFilledChunk('c'), /*version_tag=*/"");

	const InMemCacheBlock start_key {"b", /*start_off_p=*/0, /*blk_size_p=*/0};
	storage->Clear(start_key, [](const InMemCacheBlock &block) { return block.fname == "b"; });
	auto keys = storage->Keys();
	REQUIRE(keys.size() == 2);
	REQUIRE(keys[0].fname == "a");
	REQUIRE(keys[1].fname == "c");

	// Policy no longer tracks removed entries, so two more entries fit without eviction.
	storage->Put(MakeKey("d", 0), MakeFilledChunk('d'), /*version_tag=*/"");
	storage->Put(MakeKey("e", 0), MakeFilledChunk('e'), /*version_tag=*/"");
	REQUIRE(storage->Keys().size() == 4);

	storage->Clear();
	REQUIRE(storage->Keys().empty());
}

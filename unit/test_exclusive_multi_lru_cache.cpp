#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <tuple>

#include "exclusive_multi_lru_cache.hpp"

using namespace duckdb; // NOLINT

namespace {
struct MapKey {
	std::string fname;
	uint64_t off;
};
struct MapKeyEqual {
	bool operator()(const MapKey &lhs, const MapKey &rhs) const {
		return std::tie(lhs.fname, lhs.off) == std::tie(rhs.fname, rhs.off);
	}
};
struct MapKeyHash {
	std::size_t operator()(const MapKey &key) const {
		return std::hash<std::string> {}(key.fname) ^ std::hash<uint64_t> {}(key.off);
	}
};
} // namespace

TEST_CASE("PutAndGetSameKey", "[exclusive multi-lru test]") {
	ThreadSafeExclusiveMultiLruCache<std::string, std::string> cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	// No value initially.
	auto res = cache.GetAndPop("1");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(res.target_item == nullptr);
	REQUIRE(cache.Verify());

	// Check put and get.
	auto evicted = cache.Put("1", make_uniq<std::string>("1"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	res = cache.GetAndPop("1");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(*res.target_item == "1");
	REQUIRE(cache.Verify());

	// Check key eviction.
	evicted = cache.Put("1", make_uniq<std::string>("1"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	evicted = cache.Put("2", make_uniq<std::string>("2"));
	REQUIRE(*evicted == "1");
	REQUIRE(cache.Verify());

	res = cache.GetAndPop("2");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(*res.target_item == "2");
	REQUIRE(cache.Verify());
}

TEST_CASE("CustomizedStruct", "[exclusive multi-lru test]") {
	ThreadSafeExclusiveMultiLruCache<MapKey, std::string, MapKeyHash, MapKeyEqual> cache {/*max_entries_p=*/1,
	                                                                                      /*timeout_millisec_p=*/0};
	MapKey key;
	key.fname = "hello";
	key.off = 10;
	auto evicted = cache.Put(key, make_uniq<std::string>("world"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	MapKey lookup_key;
	lookup_key.fname = key.fname;
	lookup_key.off = key.off;
	auto val = cache.GetAndPop(lookup_key);
	REQUIRE(val.evicted_items.empty());
	REQUIRE(*val.target_item == "world");
	REQUIRE(cache.Verify());
}

TEST_CASE("Put items with the same key", "[exclusive multi-lru test]") {
	ThreadSafeExclusiveMultiLruCache<std::string, std::string> cache {/*max_entries_p=*/2, /*timeout_millisec_p=*/0};

	// No value initially.
	auto res = cache.GetAndPop("key");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(res.target_item == nullptr);
	REQUIRE(cache.Verify());

	// Check put and get.
	auto evicted = cache.Put("key", make_uniq<std::string>("val1"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	evicted = cache.Put("key", make_uniq<std::string>("val2"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	evicted = cache.Put("key", make_uniq<std::string>("val3"));
	REQUIRE(*evicted == "val1");
	REQUIRE(cache.Verify());

	// Check key eviction.
	res = cache.GetAndPop("key");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(*res.target_item == "val2");
	REQUIRE(cache.Verify());

	res = cache.GetAndPop("key");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(*res.target_item == "val3");
	REQUIRE(cache.Verify());

	res = cache.GetAndPop("key");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(res.target_item == nullptr);
	REQUIRE(cache.Verify());

	// Put again after empty.
	evicted = cache.Put("key", make_uniq<std::string>("val4"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	evicted = cache.Put("new-key", make_uniq<std::string>("new-val"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	// Get after put.
	res = cache.GetAndPop("non-existent");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(res.target_item == nullptr);
	REQUIRE(cache.Verify());

	res = cache.GetAndPop("new-key");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(*res.target_item == "new-val");
	REQUIRE(cache.Verify());

	res = cache.GetAndPop("key");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(*res.target_item == "val4");
	REQUIRE(cache.Verify());
}

TEST_CASE("Put and get with timeout test", "[exclusive multi-lru test]") {
	using CacheType = ThreadSafeExclusiveMultiLruCache<std::string, std::string>;

	CacheType cache {/*max_entries_p=*/4, /*timeout_millisec_p=*/500};
	auto evicted = cache.Put("key", make_uniq<std::string>("val1"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	evicted = cache.Put("key", make_uniq<std::string>("val2"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	// Sleep for a while which exceeds timeout, re-fetch key-value pair fails to get value.
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	evicted = cache.Put("key", make_uniq<std::string>("val3"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	evicted = cache.Put("key", make_uniq<std::string>("val4"));
	REQUIRE(evicted == nullptr);
	REQUIRE(cache.Verify());

	// Returned key-value pair is the fresh / unstale one.
	auto res = cache.GetAndPop("key");
	REQUIRE(res.evicted_items.size() == 2);
	REQUIRE(*res.evicted_items[0] == "val1");
	REQUIRE(*res.evicted_items[1] == "val2");
	REQUIRE(*res.target_item == "val3");
	REQUIRE(cache.Verify());

	res = cache.GetAndPop("key");
	REQUIRE(res.evicted_items.empty());
	REQUIRE(*res.target_item == "val4");
	REQUIRE(cache.Verify());
}

TEST_CASE("Evicted value test", "[exclusive multi-lru test]") {
	using CacheType = ThreadSafeExclusiveMultiLruCache<std::string, std::string>;

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};
	auto evicted = cache.Put("key1", make_uniq<std::string>("val1"));
	REQUIRE(evicted == nullptr);

	evicted = cache.Put("key2", make_uniq<std::string>("val2"));
	REQUIRE(*evicted == "val1");

	evicted = cache.Put("key3", make_uniq<std::string>("val3"));
	REQUIRE(*evicted == "val2");

	auto values = cache.ClearAndGetValues();
	REQUIRE(values.size() == 1);
	REQUIRE(*values[0] == "val3");
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

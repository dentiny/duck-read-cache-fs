#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <tuple>

#include "exclusive_lru_cache.hpp"

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

TEST_CASE("PutAndGetSameKey", "[exclusive lru test]") {
	ThreadSafeExclusiveLruCache<std::string, std::string> cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	// No value initially.
	auto val = cache.GetAndPop("1");
	REQUIRE(val == nullptr);

	// Check put and get.
	cache.Put("1", make_uniq<std::string>("1"));
	val = cache.GetAndPop("1");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "1");

	// Check key eviction.
	cache.Put("2", make_uniq<std::string>("2"));
	val = cache.GetAndPop("1");
	REQUIRE(val == nullptr);
	val = cache.GetAndPop("2");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "2");

	// Check deletion.
	REQUIRE(!cache.Delete("1"));

	cache.Put("2", make_uniq<std::string>("2"));
	REQUIRE(cache.Delete("2"));
}

TEST_CASE("CustomizedStruct", "[exclusive lru test]") {
	ThreadSafeExclusiveLruCache<MapKey, std::string, MapKeyHash, MapKeyEqual> cache {/*max_entries_p=*/1,
	                                                                                 /*timeout_millisec_p=*/0};
	MapKey key;
	key.fname = "hello";
	key.off = 10;
	auto evicted = cache.Put(key, make_uniq<std::string>("world"));
	REQUIRE(evicted == nullptr);

	MapKey lookup_key;
	lookup_key.fname = key.fname;
	lookup_key.off = key.off;
	auto val = cache.GetAndPop(lookup_key);
	REQUIRE(val != nullptr);
	REQUIRE(*val == "world");
}

TEST_CASE("Clear with filter test", "[exclusive lru test]") {
	ThreadSafeExclusiveLruCache<std::string, std::string> cache {/*max_entries_p=*/3, /*timeout_millisec_p=*/0};
	auto evicted = cache.Put("key1", make_uniq<std::string>("val1"));
	REQUIRE(evicted == nullptr);
	evicted = cache.Put("key2", make_uniq<std::string>("val2"));
	REQUIRE(evicted == nullptr);
	evicted = cache.Put("key3", make_uniq<std::string>("val3"));
	REQUIRE(evicted == nullptr);
	cache.Clear([](const std::string &key) { return key >= "key2"; });

	// Still valid keys.
	auto val = cache.GetAndPop("key1");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "val1");

	// Non-existent keys.
	val = cache.GetAndPop("key2");
	REQUIRE(val == nullptr);
	val = cache.GetAndPop("key3");
	REQUIRE(val == nullptr);
}

TEST_CASE("Put and get with timeout test", "[exclusive lru test]") {
	using CacheType = ThreadSafeExclusiveLruCache<std::string, std::string>;

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/500};
	auto evicted = cache.Put("key", make_uniq<std::string>("val"));
	REQUIRE(evicted == nullptr);

	// Getting key-value pair right afterwards is able to get the value.
	auto val = cache.GetAndPop("key");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "val");

	// Sleep for a while which exceeds timeout, re-fetch key-value pair fails to get value.
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	val = cache.GetAndPop("key");
	REQUIRE(val == nullptr);
}

TEST_CASE("Evicted value test", "[exclusive lru test]") {
	using CacheType = ThreadSafeExclusiveLruCache<std::string, std::string>;

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};
	auto evicted = cache.Put("key1", make_uniq<std::string>("val1"));
	REQUIRE(evicted == nullptr);

	evicted = cache.Put("key2", make_uniq<std::string>("val2"));
	REQUIRE(*evicted == "val1");

	evicted = cache.Put("key3", make_uniq<std::string>("val3"));
	REQUIRE(*evicted == "val2");
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

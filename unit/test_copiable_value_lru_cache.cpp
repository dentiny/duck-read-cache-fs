#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <tuple>

#include "copiable_value_lru_cache.hpp"
#include "duckdb/common/string.hpp"

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

TEST_CASE("PutAndGetSameKey", "[shared lru test]") {
	ThreadSafeCopiableValLruCache<std::string, std::string> cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	// No value initially.
	auto val = cache.Get("1");
	REQUIRE_FALSE(val);

	// Check put and get.
	cache.Put("1", std::string("1"));
	val = cache.Get("1");
	REQUIRE(val);
	REQUIRE(*val == "1");

	// Check key eviction.
	cache.Put("2", std::string("2"));
	val = cache.Get("1");
	REQUIRE_FALSE(val);
	val = cache.Get("2");
	REQUIRE(val);
	REQUIRE(*val == "2");

	// Check deletion.
	REQUIRE(!cache.Delete("1"));
	REQUIRE(cache.Delete("2"));
	val = cache.Get("2");
	REQUIRE_FALSE(val);
}

TEST_CASE("CustomizedStruct", "[shared lru test]") {
	ThreadSafeCopiableValLruCache<MapKey, std::string, MapKeyHash, MapKeyEqual> cache {/*max_entries_p=*/1,
	                                                                                   /*timeout_millisec_p=*/0};
	MapKey key;
	key.fname = "hello";
	key.off = 10;
	cache.Put(key, std::string("world"));

	MapKey lookup_key;
	lookup_key.fname = key.fname;
	lookup_key.off = key.off;
	auto val = cache.Get(lookup_key);
	REQUIRE(val);
	REQUIRE(*val == "world");
}

TEST_CASE("Clear with filter test", "[shared lru test]") {
	ThreadSafeCopiableValLruCache<std::string, std::string> cache {/*max_entries_p=*/3, /*timeout_millisec_p=*/0};
	cache.Put("key1", std::string("val1"));
	cache.Put("key2", std::string("val2"));
	cache.Put("key3", std::string("val3"));
	cache.Clear([](const std::string &key) { return key >= "key2"; });

	// Still valid keys.
	auto val = cache.Get("key1");
	REQUIRE(val);
	REQUIRE(*val == "val1");

	// Non-existent keys.
	val = cache.Get("key2");
	REQUIRE_FALSE(val);
	val = cache.Get("key3");
	REQUIRE_FALSE(val);
}

TEST_CASE("GetOrCreate test", "[shared lru test]") {
	using CacheType = ThreadSafeCopiableValLruCache<std::string, std::string>;

	std::atomic<bool> invoked = {false}; // Used to check only invoke once.
	auto factory = [&invoked](const std::string &key) -> std::string {
		REQUIRE(!invoked.exchange(true));
		// Sleep for a while so multiple threads could kick in and get blocked.
		std::this_thread::sleep_for(std::chrono::seconds(3));
		return key;
	};

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	constexpr size_t kFutureNum = 100;
	std::vector<std::future<std::string>> futures;
	futures.reserve(kFutureNum);

	const std::string key = "key";
	for (size_t idx = 0; idx < kFutureNum; ++idx) {
		futures.emplace_back(
		    std::async(std::launch::async, [&cache, &key, &factory]() { return cache.GetOrCreate(key, factory); }));
	}
	for (auto &fut : futures) {
		auto val = fut.get();
		REQUIRE(val == key);
	}

	// After we're sure key-value pair exists in cache, make one more call.
	auto cached_val = cache.GetOrCreate(key, factory);
	REQUIRE(cached_val == key);
}

TEST_CASE("Put and get with timeout test", "[shared lru test]") {
	using CacheType = ThreadSafeCopiableValLruCache<std::string, std::string>;

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/500};
	cache.Put("key", "val");

	// Getting key-value pair right afterwards is able to get the value.
	auto val = cache.Get("key");
	REQUIRE(val);
	REQUIRE(*val == "val");

	// Sleep for a while which exceeds timeout, re-fetch key-value pair fails to get value.
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	val = cache.Get("key");
	REQUIRE_FALSE(val);
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

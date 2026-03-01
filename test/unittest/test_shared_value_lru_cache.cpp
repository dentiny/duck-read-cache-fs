#include "catch/catch.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <tuple>

#include "duckdb/common/string.hpp"
#include "shared_value_lru_cache.hpp"

using namespace duckdb; // NOLINT

namespace {
struct MapKey {
	std::string fname;
	uint64_t off;
};
struct MapKeyLess {
	bool operator()(const MapKey &lhs, const MapKey &rhs) const {
		return std::tie(lhs.fname, lhs.off) < std::tie(rhs.fname, rhs.off);
	}
};
struct MapKeyEqual {
	bool operator()(const MapKey &lhs, const MapKey &rhs) const {
		return std::tie(lhs.fname, lhs.off) == std::tie(rhs.fname, rhs.off);
	}
};
} // namespace

TEST_CASE("SharedValueLru PutAndGetSameKey", "[shared value lru test]") {
	ThreadSafeSharedValueLruCache<std::string, std::string> cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	// No value initially.
	auto val = cache.Get("1");
	REQUIRE(val == nullptr);

	// Check put and get.
	cache.Put("1", make_shared_ptr<std::string>("1"));
	val = cache.Get("1");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "1");

	// Check key eviction.
	cache.Put("2", make_shared_ptr<std::string>("2"));
	val = cache.Get("1");
	REQUIRE(val == nullptr);
	val = cache.Get("2");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "2");

	// Check deletion.
	REQUIRE(!cache.Delete("1"));
	REQUIRE(cache.Delete("2"));
	val = cache.Get("2");
	REQUIRE(val == nullptr);
}

TEST_CASE("SharedValueLru CustomizedStruct", "[shared value lru test]") {
	ThreadSafeSharedValueLruCache<MapKey, std::string, MapKeyLess, MapKeyEqual> cache {/*max_entries_p=*/1,
	                                                                                   /*timeout_millisec_p=*/0};
	MapKey key;
	key.fname = "hello";
	key.off = 10;
	cache.Put(key, make_shared_ptr<std::string>("world"));

	MapKey lookup_key;
	lookup_key.fname = key.fname;
	lookup_key.off = key.off;
	auto val = cache.Get(lookup_key);
	REQUIRE(val != nullptr);
	REQUIRE(*val == "world");
}

TEST_CASE("SharedValueLru Clear with filter", "[shared value lru test]") {
	ThreadSafeSharedValueLruCache<std::string, std::string> cache {/*max_entries_p=*/3, /*timeout_millisec_p=*/0};
	cache.Put("key1", make_shared_ptr<std::string>("val1"));
	cache.Put("key2", make_shared_ptr<std::string>("val2"));
	cache.Put("key3", make_shared_ptr<std::string>("val3"));
	cache.Clear("key2", [](const std::string &key) { return key >= "key2"; });

	// Still valid keys.
	auto val = cache.Get("key1");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "val1");

	// Non-existent keys.
	val = cache.Get("key2");
	REQUIRE(val == nullptr);
	val = cache.Get("key3");
	REQUIRE(val == nullptr);
}

TEST_CASE("SharedValueLru GetOrCreate", "[shared value lru test]") {
	using CacheType = ThreadSafeSharedValueLruCache<std::string, std::string>;

	std::atomic<bool> invoked = {false}; // Used to check only invoke once.
	auto factory = [&invoked](const std::string &key) -> shared_ptr<std::string> {
		REQUIRE(!invoked.exchange(true));
		// Sleep for a while so multiple threads could kick in and get blocked.
		std::this_thread::sleep_for(std::chrono::seconds(3));
		return make_shared_ptr<std::string>(key);
	};

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	constexpr size_t kFutureNum = 100;
	std::vector<std::future<shared_ptr<std::string>>> futures;
	futures.reserve(kFutureNum);

	const std::string key = "key";
	for (size_t idx = 0; idx < kFutureNum; ++idx) {
		futures.emplace_back(
		    std::async(std::launch::async, [&cache, &key, &factory]() { return cache.GetOrCreate(key, factory); }));
	}
	for (auto &fut : futures) {
		auto val = fut.get();
		REQUIRE(val != nullptr);
		REQUIRE(*val == key);
	}

	// After we're sure key-value pair exists in cache, make one more call.
	auto cached_val = cache.GetOrCreate(key, factory);
	REQUIRE(cached_val != nullptr);
	REQUIRE(*cached_val == key);
}

TEST_CASE("SharedValueLru Put and get with timeout", "[shared value lru test]") {
	using CacheType = ThreadSafeSharedValueLruCache<std::string, std::string>;

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/500};
	cache.Put("key", make_shared_ptr<std::string>("val"));

	// Getting key-value pair right afterwards is able to get the value.
	auto val = cache.Get("key");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "val");

	// Sleep for a while which exceeds timeout, re-fetch key-value pair fails to get value.
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	val = cache.Get("key");
	REQUIRE(val == nullptr);
}

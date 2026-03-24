#include "catch/catch.hpp"

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
	string fname;
	uint64_t off;
};
struct MapKeyEqual {
	bool operator()(const MapKey &lhs, const MapKey &rhs) const {
		return std::tie(lhs.fname, lhs.off) == std::tie(rhs.fname, rhs.off);
	}
};
struct MapKeyHash {
	std::size_t operator()(const MapKey &key) const {
		return std::hash<string> {}(key.fname) ^ std::hash<uint64_t> {}(key.off);
	}
};
} // namespace

TEST_CASE("CopiableValueLruCache_PutAndGetSameKey", "[shared lru test]") {
	ThreadSafeCopiableValLruCache<string, string> cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	// No value initially.
	auto val = cache.Get("1");
	REQUIRE_FALSE(val);

	// Check put and get.
	cache.Put("1", string("1"));
	val = cache.Get("1");
	REQUIRE(val);
	REQUIRE(*val == "1");

	// Check key eviction.
	cache.Put("2", string("2"));
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

TEST_CASE("CopiableValueLru CustomizedStruct", "[shared lru test]") {
	ThreadSafeCopiableValLruCache<MapKey, string, MapKeyHash, MapKeyEqual> cache {/*max_entries_p=*/1,
	                                                                              /*timeout_millisec_p=*/0};
	MapKey key;
	key.fname = "hello";
	key.off = 10;
	cache.Put(key, string("world"));

	MapKey lookup_key;
	lookup_key.fname = key.fname;
	lookup_key.off = key.off;
	auto val = cache.Get(lookup_key);
	REQUIRE(val);
	REQUIRE(*val == "world");
}

TEST_CASE("CopiableValueLru clear with filter test", "[shared lru test]") {
	ThreadSafeCopiableValLruCache<string, string> cache {/*max_entries_p=*/3, /*timeout_millisec_p=*/0};
	cache.Put("key1", string("val1"));
	cache.Put("key2", string("val2"));
	cache.Put("key3", string("val3"));
	cache.Clear([](const string &key) { return key >= "key2"; });

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

TEST_CASE("CopiableValueLru GetOrCreate", "[shared lru test]") {
	using CacheType = ThreadSafeCopiableValLruCache<string, string>;

	std::atomic<bool> invoked = {false}; // Used to check only invoke once.
	auto factory = [&invoked](const string &key) -> string {
		REQUIRE(!invoked.exchange(true));
		// Sleep for a while so multiple threads could kick in and get blocked.
		std::this_thread::sleep_for(std::chrono::seconds(3));
		return key;
	};

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	constexpr size_t FUTURE_NUMBER = 100;
	std::vector<std::future<string>> futures;
	futures.reserve(FUTURE_NUMBER);

	const string key = "key";
	for (size_t idx = 0; idx < FUTURE_NUMBER; ++idx) {
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

TEST_CASE("CopiableValueLru Put and get with timeout", "[shared lru test]") {
	using CacheType = ThreadSafeCopiableValLruCache<string, string>;

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

TEST_CASE("CopiableValueLru GetOrCreate factory exception cleans up", "[shared value lru test]") {
	using CacheType = ThreadSafeCopiableValLruCache<string, string>;
	CacheType cache {/*max_entries=*/10, /*timeout_millisec=*/0};
	const string key = "key";

	// First attempt fails with an exception.
	auto throwing_factory = [](const string &) -> string {
		throw std::runtime_error("factory failed");
	};
	REQUIRE_THROWS_AS(cache.GetOrCreate(key, throwing_factory), std::runtime_error);

	// After an exception, the creation token should be cleaned up so a subsequent call should attempt a new request.
	auto good_factory = [](const string &k) -> string {
		return string {k};
	};
	auto result = cache.GetOrCreate(key, good_factory);
	REQUIRE(result == key);
}

TEST_CASE("CopiableValueLru GetOrCreate exception propagates to waiting threads", "[shared value lru test]") {
	using CacheType = ThreadSafeCopiableValLruCache<string, string>;
	CacheType cache {/*max_entries=*/10, /*timeout_millisec=*/0};
	const string key = "key";

	auto throwing_factory = [](const string &) -> string {
		// Sleep for a while so multiple threads could kick in and get blocked.
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
		throw std::runtime_error("factory failed");
	};

	constexpr size_t FUTURE_NUMBER = 10;
	vector<std::future<string>> futures;
	futures.reserve(FUTURE_NUMBER);

	for (size_t idx = 0; idx < FUTURE_NUMBER; ++idx) {
		futures.emplace_back(std::async(std::launch::async, [&cache, &key, &throwing_factory]() {
			return cache.GetOrCreate(key, throwing_factory);
		}));
	}

	for (auto &fut : futures) {
		REQUIRE_THROWS_AS(fut.get(), std::runtime_error);
	}

	// Cache should be usable after all threads received the exception.
	auto good_factory = [](const string &k) -> string {
		return string {k};
	};
	auto result = cache.GetOrCreate(key, good_factory);
	REQUIRE(result == key);
}

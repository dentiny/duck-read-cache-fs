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
	string fname;
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
	ThreadSafeSharedValueLruCache<string, string> cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	// No value initially.
	auto val = cache.Get("1");
	REQUIRE(val == nullptr);

	// Check put and get.
	cache.Put("1", make_shared_ptr<string>("1"));
	val = cache.Get("1");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "1");

	// Check key eviction.
	cache.Put("2", make_shared_ptr<string>("2"));
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
	ThreadSafeSharedValueLruCache<MapKey, string, MapKeyLess> cache {/*max_entries_p=*/1,
	                                                                 /*timeout_millisec_p=*/0};
	MapKey key;
	key.fname = "hello";
	key.off = 10;
	cache.Put(key, make_shared_ptr<string>("world"));

	MapKey lookup_key;
	lookup_key.fname = key.fname;
	lookup_key.off = key.off;
	auto val = cache.Get(lookup_key);
	REQUIRE(val != nullptr);
	REQUIRE(*val == "world");
}

TEST_CASE("SharedValueLru Clear with filter", "[shared value lru test]") {
	ThreadSafeSharedValueLruCache<string, string> cache {/*max_entries_p=*/3, /*timeout_millisec_p=*/0};
	cache.Put("key1", make_shared_ptr<string>("val1"));
	cache.Put("key2", make_shared_ptr<string>("val2"));
	cache.Put("key3", make_shared_ptr<string>("val3"));
	cache.Clear("key2", [](const string &key) { return key >= "key2"; });

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
	using CacheType = ThreadSafeSharedValueLruCache<string, string>;

	std::atomic<bool> invoked = {false}; // Used to check only invoke once.
	auto factory = [&invoked](const string &key) -> shared_ptr<string> {
		REQUIRE(!invoked.exchange(true));
		// Sleep for a while so multiple threads could kick in and get blocked.
		std::this_thread::sleep_for(std::chrono::seconds(3));
		return make_shared_ptr<string>(key);
	};

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/0};

	constexpr size_t FUTURE_NUMBER = 100;
	std::vector<std::future<shared_ptr<string>>> futures;
	futures.reserve(FUTURE_NUMBER);

	const string key = "key";
	for (size_t idx = 0; idx < FUTURE_NUMBER; ++idx) {
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
	using CacheType = ThreadSafeSharedValueLruCache<string, string>;

	CacheType cache {/*max_entries_p=*/1, /*timeout_millisec_p=*/500};
	cache.Put("key", make_shared_ptr<string>("val"));

	// Getting key-value pair right afterwards is able to get the value.
	auto val = cache.Get("key");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "val");

	// Sleep for a while which exceeds timeout, re-fetch key-value pair fails to get value.
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	val = cache.Get("key");
	REQUIRE(val == nullptr);
}

TEST_CASE("SharedValueLru GetOrCreate factory exception cleans up", "[shared value lru test]") {
	using CacheType = ThreadSafeSharedValueLruCache<string, string>;
	CacheType cache {/*max_entries=*/10, /*timeout_millisec=*/0};
	const string key = "key";

	// First attempt fails with an exception.
	auto throwing_factory = [](const string &) -> shared_ptr<string> {
		throw std::runtime_error("factory failed");
	};
	REQUIRE_THROWS_AS(cache.GetOrCreate(key, throwing_factory), std::runtime_error);

	// After an exception, the creation token should be cleaned up so a subsequent call should attempt a new request.
	auto good_factory = [](const string &k) -> shared_ptr<string> {
		return make_shared_ptr<string>(k);
	};
	auto result = cache.GetOrCreate(key, good_factory);
	REQUIRE(*result == key);
}

TEST_CASE("SharedValueLru GetOrCreate exception propagates to waiting threads", "[shared value lru test]") {
	using CacheType = ThreadSafeSharedValueLruCache<string, string>;
	CacheType cache {/*max_entries=*/10, /*timeout_millisec=*/0};
	const string key = "key";

	auto throwing_factory = [](const string &) -> shared_ptr<string> {
		// Sleep for a while so multiple threads could kick in and get blocked.
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
		throw std::runtime_error("factory failed");
	};

	constexpr size_t FUTURE_NUMBER = 10;
	vector<std::future<shared_ptr<string>>> futures;
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
	auto good_factory = [](const string &k) -> shared_ptr<string> {
		return make_shared_ptr<string>(k);
	};
	auto result = cache.GetOrCreate(key, good_factory);
	REQUIRE(result != nullptr);
	REQUIRE(*result == key);
}

TEST_CASE("SharedValueLru DuplicateKeyPut", "[exclusive lru test]") {
	using CacheType = ThreadSafeSharedValueLruCache<string, string>;
	CacheType cache {/*max_entries_p=*/3, /*timeout_millisec_p=*/0};

	cache.Put("a", make_shared_ptr<string>("a1"));
	cache.Put("b", make_shared_ptr<string>("b1"));
	cache.Put("c", make_shared_ptr<string>("c1"));

	// Insert duplicate keys.
	cache.Put("a", make_shared_ptr<string>("a2"));
	cache.Put("a", make_shared_ptr<string>("a3"));

	// The latest value for "a" must be visible.
	auto val = cache.Get("a");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "a3");

	val = cache.Get("b");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "b1");

	val = cache.Get("c");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "c1");

	auto keys = cache.Keys();
	REQUIRE(keys.size() == 3);
}

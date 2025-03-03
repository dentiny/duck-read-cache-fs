#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <atomic>
#include <future>
#include <string>
#include <thread>
#include <tuple>

#include "copiable_value_lru_cache.hpp"

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
	ThreadSafeCopiableValLruCache<std::string, std::string> cache {1};

	// No value initially.
	auto val = cache.Get("1");
	REQUIRE(val.empty());

	// Check put and get.
	cache.Put("1", std::string("1"));
	val = cache.Get("1");
	REQUIRE(!val.empty());
	REQUIRE(val == "1");

	// Check key eviction.
	cache.Put("2", std::string("2"));
	val = cache.Get("1");
	REQUIRE(val.empty());
	val = cache.Get("2");
	REQUIRE(!val.empty());
	REQUIRE(val == "2");

	// Check deletion.
	REQUIRE(!cache.Delete("1"));
	REQUIRE(cache.Delete("2"));
	val = cache.Get("2");
	REQUIRE(val.empty());
}

TEST_CASE("CustomizedStruct", "[shared lru test]") {
	ThreadSafeCopiableValLruCache<MapKey, std::string, MapKeyHash, MapKeyEqual> cache {1};
	MapKey key;
	key.fname = "hello";
	key.off = 10;
	cache.Put(key, std::string("world"));

	MapKey lookup_key;
	lookup_key.fname = key.fname;
	lookup_key.off = key.off;
	auto val = cache.Get(lookup_key);
	REQUIRE(!val.empty());
	REQUIRE(val == "world");
}

TEST_CASE("Clear with filter test", "[shared lru test]") {
	ThreadSafeCopiableValLruCache<std::string, std::string> cache {3};
	cache.Put("key1", std::string("val1"));
	cache.Put("key2", std::string("val2"));
	cache.Put("key3", std::string("val3"));
	cache.Clear([](const std::string &key) { return key >= "key2"; });

	// Still valid keys.
	auto val = cache.Get("key1");
	REQUIRE(!val.empty());
	REQUIRE(val == "val1");

	// Non-existent keys.
	val = cache.Get("key2");
	REQUIRE(val.empty());
	val = cache.Get("key3");
	REQUIRE(val.empty());
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

	CacheType cache {1};

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

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

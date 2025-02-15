#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <atomic>
#include <future>
#include <string>
#include <thread>
#include <tuple>

#include "lru_cache.hpp"

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
	ThreadSafeSharedLruCache<std::string, std::string> cache {1};

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

TEST_CASE("CustomizedStruct", "[shared lru test]") {
	ThreadSafeSharedLruCache<MapKey, std::string, MapKeyHash, MapKeyEqual> cache {1};
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

TEST_CASE("GetOrCreate test", "[shared lru test]") {
	using CacheType = ThreadSafeSharedLruCache<std::string, std::string>;

	std::atomic<bool> invoked = {false}; // Used to check only invoke once.
	auto factory = [&invoked](const std::string &key) -> shared_ptr<std::string> {
		REQUIRE(!invoked.exchange(true));
		// Sleep for a while so multiple threads could kick in and get blocked.
		std::this_thread::sleep_for(std::chrono::seconds(3));
		return make_shared_ptr<std::string>(key);
	};

	CacheType cache {1};

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

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

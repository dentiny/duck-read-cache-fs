#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <string>
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
	cache.Put("1", std::make_shared<std::string>("1"));
	val = cache.Get("1");
	REQUIRE(val != nullptr);
	REQUIRE(*val == "1");

	// Check key eviction.
	cache.Put("2", std::make_shared<std::string>("2"));
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
	cache.Put(key, std::make_shared<std::string>("world"));

	MapKey lookup_key;
	lookup_key.fname = key.fname;
	lookup_key.off = key.off;
	auto val = cache.Get(lookup_key);
	REQUIRE(val != nullptr);
	REQUIRE(*val == "world");
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

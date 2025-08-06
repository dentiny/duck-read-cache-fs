#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "counter.hpp"
#include "duckdb/common/assert.hpp"

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

TEST_CASE("IncrementAndDecrement", "[counter test]") {
	ThreadSafeCounter<std::string> counter {};
	REQUIRE(counter.Increment("key") == 1);
	REQUIRE(counter.GetCount("key") == 1);
	REQUIRE(counter.Decrement("key") == 0);
	REQUIRE(counter.GetCount("key") == 0);
}

TEST_CASE("CounterWithCustomizedKey", "[counter test]") {
	ThreadSafeCounter<MapKey, MapKeyHash, MapKeyEqual> counter {};
	MapKey key;
	key.fname = "hello";
	key.off = 10;
	REQUIRE(counter.Increment(key) == 1);
	REQUIRE(counter.GetCount(key) == 1);
	REQUIRE(counter.Decrement(key) == 0);
	REQUIRE(counter.GetCount(key) == 0);
}

TEST_CASE("ClearCounterWithPred", "[counter test]") {
	ThreadSafeCounter<MapKey, MapKeyHash, MapKeyEqual> counter {};
	MapKey key;
	key.fname = "hello";
	key.off = 10;
	REQUIRE(counter.Increment(key) == 1);
	REQUIRE(counter.GetCount(key) == 1);

	counter.ClearCounter([](const MapKey &arg) { return false; });
	REQUIRE(counter.GetCount(key) == 1);
	counter.ClearCounter([&key](const MapKey &arg) { return MapKeyEqual {}.operator()(key, arg); });
	REQUIRE(counter.GetCount(key) == 0);
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

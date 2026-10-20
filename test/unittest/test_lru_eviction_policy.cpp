#include "catch/catch.hpp"

#include "lru_eviction_policy.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("LruEvictionPolicy evicts least recently used key", "[cache eviction policy test]") {
	LruEvictionPolicy<string> policy {/*max_entries_p=*/2};
	REQUIRE(policy.OnInsert("a").empty());
	REQUIRE(policy.OnInsert("b").empty());

	// Access "a", so "b" becomes the least recently used one.
	policy.OnAccess("a");
	REQUIRE(policy.OnInsert("c") == vector<string> {"b"});
	REQUIRE(policy.OnInsert("d") == vector<string> {"a"});
}

TEST_CASE("LruEvictionPolicy removal frees capacity", "[cache eviction policy test]") {
	LruEvictionPolicy<string> policy {/*max_entries_p=*/2};
	REQUIRE(policy.OnInsert("a").empty());
	REQUIRE(policy.OnInsert("b").empty());
	policy.OnRemove("a");
	REQUIRE(policy.OnInsert("c").empty());
	REQUIRE(policy.OnInsert("d") == vector<string> {"b"});

	policy.Clear();
	REQUIRE(policy.OnInsert("e").empty());
	REQUIRE(policy.OnInsert("f").empty());
}

TEST_CASE("LruEvictionPolicy unbounded", "[cache eviction policy test]") {
	LruEvictionPolicy<string> policy {/*max_entries_p=*/0};
	for (int idx = 0; idx < 100; ++idx) {
		REQUIRE(policy.OnInsert(std::to_string(idx)).empty());
	}
}

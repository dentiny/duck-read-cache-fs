#include "catch/catch.hpp"

#include "duckdb/common/string.hpp"
#include "lru_eviction_policy.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("LruEvictionPolicy evicts least recently used key", "[lru eviction policy test]") {
	LruEvictionPolicy<string> policy;
	REQUIRE_FALSE(policy.PopVictim());

	policy.OnInsert("a");
	policy.OnInsert("b");
	policy.OnInsert("c");

	// Access "a", so "b" becomes the least recently used one.
	policy.OnAccess("a");
	REQUIRE(*policy.PopVictim() == "b");
	REQUIRE(*policy.PopVictim() == "c");
	REQUIRE(*policy.PopVictim() == "a");
	REQUIRE_FALSE(policy.PopVictim());
}

TEST_CASE("LruEvictionPolicy tracks keys", "[lru eviction policy test]") {
	LruEvictionPolicy<string> policy;
	policy.OnInsert("a");
	policy.OnInsert("b");
	REQUIRE(policy.Contains("a"));

	policy.OnRemove("a");
	REQUIRE_FALSE(policy.Contains("a"));
	REQUIRE(*policy.PopVictim() == "b");
	REQUIRE_FALSE(policy.Contains("b"));

	policy.OnInsert("c");
	policy.Clear();
	REQUIRE_FALSE(policy.Contains("c"));
	REQUIRE_FALSE(policy.PopVictim());
}

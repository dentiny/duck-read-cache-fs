#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_exclusion_manager.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Exclusion test", "[cache exclusion manager test]") {
	auto &exclusion_manager = CacheExclusionManager::GetInstance();

	// A non-match-all regex.
	exclusion_manager.AddExlusionRegex(".*config.*");
	REQUIRE(!exclusion_manager.MatchAnyExclusion("non-match-file"));
	REQUIRE(exclusion_manager.MatchAnyExclusion("/tmp/config"));

	// A match-all regex.
	exclusion_manager.AddExlusionRegex(".*");
	REQUIRE(exclusion_manager.MatchAnyExclusion("match-file"));
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem_config.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Filesystem config test", "[filesystem config]") {
	REQUIRE(GetThreadCountForSubrequests(10) == 10);

	g_max_subrequest_count = 5;
	REQUIRE(GetThreadCountForSubrequests(10) == 5);
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "string_utils.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Test is glob expression", "[string utils test]") {
	REQUIRE(!IsGlobExpression("/tmp/filename"));
	REQUIRE(IsGlobExpression("/tmp/*"));
	REQUIRE(IsGlobExpression("/tmp/[abc]"));
	REQUIRE(IsGlobExpression("/tmp/a.b?"));
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

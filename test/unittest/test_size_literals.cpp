#include "catch/catch.hpp"

#include "size_literals.hpp"

TEST_CASE("Size literals test", "[size literals]") {
	REQUIRE(2_MiB == 2 * 1024 * 1024);
	REQUIRE(2.5_KB == 2500);
	REQUIRE(4_GB == 4000000000);
}

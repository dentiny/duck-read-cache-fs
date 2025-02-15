#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "histogram.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Histogram test", "[histogram test]") {
	Histogram hist {/*min_val=*/0, /*max_val=*/10, /*num_bkt=*/10};
	hist.Add(1);
	hist.Add(3);
	hist.Add(-3);
	REQUIRE(hist.outliers() == std::vector<double> {-3});
	REQUIRE(hist.min() == 1);
	REQUIRE(hist.max() == 3);
	REQUIRE(hist.counts() == 2);
	REQUIRE(hist.mean() == 2);
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

#include "catch/catch.hpp"

#include "histogram.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Histogram custom boundaries test", "[histogram test]") {
	// Buckets: [0,1) [1,10) [10,100) [100,1000) [1000,+inf)
	Histogram hist {vector<double> {1, 10, 100, 1000}};

	hist.Add(0.5);  // bucket 0: [0, 1)
	hist.Add(5);    // bucket 1: [1, 10)
	hist.Add(50);   // bucket 2: [10, 100)
	hist.Add(500);  // bucket 3: [100, 1000)
	hist.Add(5000); // bucket 4: [1000, +inf)

	REQUIRE(hist.counts() == 5);
	REQUIRE(hist.min() == 0.5);
	REQUIRE(hist.max() == 5000);

	// Verify bucket indices.
	REQUIRE(hist.Bucket(0.5) == 0);
	REQUIRE(hist.Bucket(5) == 1);
	REQUIRE(hist.Bucket(50) == 2);
	REQUIRE(hist.Bucket(500) == 3);
	REQUIRE(hist.Bucket(5000) == 4);

	// Reset and verify clean state.
	hist.Reset();
	REQUIRE(hist.counts() == 0);
}

TEST_CASE("Histogram custom boundaries edge cases", "[histogram test]") {
	// Buckets: [0,10) [10,100) [100,+inf)
	Histogram hist {vector<double> {10, 100}};

	// Boundary value goes to next bucket.
	hist.Add(10);
	REQUIRE(hist.Bucket(10) == 1);

	// Negative value goes to first bucket.
	hist.Add(-5);
	REQUIRE(hist.Bucket(-5) == 0);

	REQUIRE(hist.counts() == 2);
}

TEST_CASE("Histogram mean and sum", "[histogram test]") {
	Histogram hist {vector<double> {10}};
	hist.Add(2);
	hist.Add(4);
	hist.Add(6);
	REQUIRE(hist.counts() == 3);
	REQUIRE(hist.sum() == 12);
	REQUIRE(hist.mean() == 4);
	REQUIRE(hist.min() == 2);
	REQUIRE(hist.max() == 6);
}

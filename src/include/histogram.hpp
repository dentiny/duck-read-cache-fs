#pragma once

#include <cstddef>

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Histogram with non-uniform bucket boundaries
// Each boundary defines the upper bound of a bucket; an overflow bucket is appended for values beyond the last
// boundary. Example: boundaries = {1, 10, 100} creates buckets [0,1), [1,10), [10,100), [100,+inf).
// All values are recorded into buckets (no outliers).
class Histogram {
public:
	explicit Histogram(vector<double> boundaries);

	Histogram(const Histogram &) = delete;
	Histogram &operator=(const Histogram &) = delete;
	Histogram(Histogram &&) = delete;
	Histogram &operator=(Histogram &&) = delete;

	// Set the distribution stats name and unit, used for formatting purpose.
	void SetStatsDistribution(string name, string unit);

	// Add [val] into the histogram.
	void Add(double val);

	// Get bucket index for the given [val].
	size_t Bucket(double val) const;

	// Stats data.
	size_t counts() const {
		return total_counts_;
	}
	double sum() const {
		return sum_;
	}
	double mean() const;
	// Precondition: there's at least one value inserted.
	double min() const {
		return min_encountered_;
	}
	double max() const {
		return max_encountered_;
	}

	// Display histogram into string format.
	string FormatString() const;

	// Reset histogram.
	void Reset();

private:
	// Sorted bucket boundaries.
	vector<double> boundaries_;
	// Max and min value encountered.
	double min_encountered_;
	double max_encountered_;
	// Total number of values.
	size_t total_counts_ = 0;
	// Accumulated sum.
	double sum_ = 0.0;
	// List of bucket counts.
	vector<size_t> hist_;
	// Item name and unit for stats distribution.
	string distribution_name_;
	string distribution_unit_;
};

} // namespace duckdb

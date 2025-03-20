#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace duckdb {

// Historgram supports two types of records
// - For values within the given range, all the stats functions (i.e. min and max) only considers in-range values;
// - For values out of range, we provide extra functions to retrieve.
//
// The reason why outliers are not considered as statistic is they disturb statistical value a lot.
class Histogram {
public:
	// [min_val] is inclusive, and [max_val] is exclusive.
	Histogram(double min_val, double max_val, int num_bkt);

	Histogram(const Histogram &) = delete;
	Histogram &operator=(const Histogram &) = delete;
	Histogram(Histogram &&) = delete;
	Histogram &operator=(Histogram &&) = delete;

	// Set the distribution stats name and unit, used for formatting purpose.
	void SetStatsDistribution(std::string name, std::string unit);

	// Add [val] into the histogram.
	// Return whether [val] is valid.
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

	// Get outliers for stat records.
	const std::vector<double> outliers() const {
		return outliers_;
	}

	// Display histogram into string format.
	std::string FormatString() const;

	// Reset histogram.
	void Reset();

private:
	const double min_val_;
	const double max_val_;
	const int num_bkt_;
	// Max and min value encountered.
	double min_encountered_;
	double max_encountered_;
	// Total number of values.
	size_t total_counts_ = 0;
	// Accumulated sum.
	double sum_ = 0.0;
	// List of bucket counts.
	std::vector<size_t> hist_;
	// List of outliers.
	std::vector<double> outliers_;
	// Item name and unit for stats distribution.
	std::string distribution_name_;
	std::string distribution_unit_;
};

} // namespace duckdb

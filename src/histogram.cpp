#include "histogram.hpp"

#include <algorithm>
#include <limits>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

Histogram::Histogram(vector<double> boundaries) {
	if (boundaries.empty()) {
		throw InvalidInputException("Histogram boundaries must not be empty");
	}
	boundaries_ = std::move(boundaries);
	D_ASSERT(std::is_sorted(boundaries_.begin(), boundaries_.end()));
	Reset();
}

void Histogram::SetStatsDistribution(string name, string unit) {
	distribution_name_ = std::move(name);
	distribution_unit_ = std::move(unit);
}

void Histogram::Reset() {
	min_encountered_ = std::numeric_limits<double>::max();
	max_encountered_ = std::numeric_limits<double>::lowest();
	total_counts_ = 0;
	sum_ = 0;
	// Bucket count = boundaries count + 1 (last bucket is overflow: [last_boundary, +inf)).
	hist_ = vector<size_t>(boundaries_.size() + 1, 0);
}

size_t Histogram::Bucket(double val) const {
	auto it = std::upper_bound(boundaries_.begin(), boundaries_.end(), val);
	return static_cast<size_t>(it - boundaries_.begin());
}

void Histogram::Add(double val) {
	++hist_[Bucket(val)];
	min_encountered_ = std::min(min_encountered_, val);
	max_encountered_ = std::max(max_encountered_, val);
	++total_counts_;
	sum_ += val;
}

double Histogram::mean() const {
	if (total_counts_ == 0) {
		return 0.0;
	}
	return sum_ / total_counts_;
}

string Histogram::FormatString() const {
	string res;

	// Format aggregated stats.
	res += StringUtil::Format("Max %s = %lf %s\n", distribution_name_, max(), distribution_unit_);
	res += StringUtil::Format("Min %s = %lf %s\n", distribution_name_, min(), distribution_unit_);
	res += StringUtil::Format("Mean %s = %lf %s\n", distribution_name_, mean(), distribution_unit_);

	// Format stats distribution.
	for (size_t idx = 0; idx < hist_.size(); ++idx) {
		if (hist_[idx] == 0) {
			continue;
		}
		const double cur_min_val = (idx == 0) ? 0.0 : boundaries_[idx - 1];
		const bool is_last = (idx == boundaries_.size());
		const double percentage = hist_[idx] * 1.0 / total_counts_ * 100;
		if (is_last) {
			res += StringUtil::Format("Distribution %s [%lf, +inf) %s: %lf %%\n", distribution_name_, cur_min_val,
			                          distribution_unit_, percentage);
		} else {
			res += StringUtil::Format("Distribution %s [%lf, %lf) %s: %lf %%\n", distribution_name_, cur_min_val,
			                          boundaries_[idx], distribution_unit_, percentage);
		}
	}

	return res;
}

} // namespace duckdb

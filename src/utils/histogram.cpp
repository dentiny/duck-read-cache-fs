#include "histogram.hpp"

#include <algorithm>
#include <cmath>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

Histogram::Histogram(double min_val, double max_val, int num_bkt) : min_val_(min_val), max_val_(max_val) {
	D_ASSERT(min_val_ < max_val_);
	D_ASSERT(num_bkt > 0);
	hist_.resize(num_bkt);

	min_encountered_ = max_val_;
	max_encountered_ = min_val_;
}

size_t Histogram::Bucket(double val) const {
	D_ASSERT(val >= min_val_);
	D_ASSERT(val <= max_val_);

	if (val == max_val_) {
		return hist_.size() - 1;
	}
	const double idx = (val - min_val_) / (max_val_ - min_val_);
	return static_cast<size_t>(std::floor(idx * hist_.size()));
}

void Histogram::Add(double val) {
	if (val < min_val_ || val > max_val_) {
		outliers_.emplace_back(val);
		return;
	}
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

// TODO(hjiang): Add buckets into formatted string.
std::string Histogram::FormatString() const {
	std::string res;

	// Format outliers.
	if (!outliers_.empty()) {
		auto double_to_string = [](double v) -> string {
			return StringUtil::Format("%lf", v);
		};
		res =
		    StringUtil::Format("Outliers: %s\n", StringUtil::Join(outliers_, outliers_.size(), ", ", double_to_string));
	}

	// Format aggregated stats.
	res += StringUtil::Format("Max = %lf\n", max());
	res += StringUtil::Format("Min = %lf\n", min());
	res += StringUtil::Format("Mean = %lf\n", mean());

	return res;
}

} // namespace duckdb

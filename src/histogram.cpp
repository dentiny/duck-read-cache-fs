#include "histogram.hpp"

#include <algorithm>
#include <cmath>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

Histogram::Histogram(double min_val, double max_val, int num_bkt)
    : min_val_(min_val), max_val_(max_val), num_bkt_(num_bkt) {
	D_ASSERT(min_val_ < max_val_);
	D_ASSERT(num_bkt > 0);
	Reset();
}

void Histogram::SetStatsDistribution(std::string name, std::string unit) {
	distribution_name_ = std::move(name);
	distribution_unit_ = std::move(unit);
}

void Histogram::Reset() {
	min_encountered_ = max_val_;
	max_encountered_ = min_val_;
	total_counts_ = 0;
	sum_ = 0;
	hist_ = std::vector<size_t>(num_bkt_, 0);
	outliers_.clear();
}

size_t Histogram::Bucket(double val) const {
	D_ASSERT(val >= min_val_);
	D_ASSERT(val < max_val_);

	if (val == max_val_) {
		return hist_.size() - 1;
	}
	const double idx = (val - min_val_) / (max_val_ - min_val_);
	return static_cast<size_t>(std::floor(idx * hist_.size()));
}

void Histogram::Add(double val) {
	if (val < min_val_ || val >= max_val_) {
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

std::string Histogram::FormatString() const {
	std::string res;

	// Format outliers.
	if (!outliers_.empty()) {
		auto double_to_string = [](double v) -> string {
			return StringUtil::Format("%lf", v);
		};
		res = StringUtil::Format("Outliers %s with unit %s: %s\n", distribution_name_, distribution_unit_,
		                         StringUtil::Join(outliers_, outliers_.size(), ", ", double_to_string));
	}

	// Format aggregated stats.
	res += StringUtil::Format("Max %s = %lf %s\n", distribution_name_, max(), distribution_unit_);
	res += StringUtil::Format("Min %s = %lf %s\n", distribution_name_, min(), distribution_unit_);
	res += StringUtil::Format("Mean %s = %lf %s\n", distribution_name_, mean(), distribution_unit_);

	// Format stats distribution.
	const double interval = (max_val_ - min_val_) / num_bkt_;
	for (size_t idx = 0; idx < hist_.size(); ++idx) {
		// Skip empty bucket.
		if (hist_[idx] == 0) {
			continue;
		}
		const double cur_min_val = min_val_ + interval * idx;
		const double cur_max_val = MinValue<double>(cur_min_val + interval, max_val_);
		const double percentage = hist_[idx] * 1.0 / total_counts_ * 100;
		res += StringUtil::Format("Distribution %s [%lf, %lf) %s: %lf %%\n", distribution_name_, cur_min_val,
		                          cur_max_val, distribution_unit_, percentage);
	}

	return res;
}

} // namespace duckdb

// Profile collector, which profiles and stores the latest result in memory.

#pragma once

#include "base_profile_collector.hpp"
#include "duckdb/common/profiler.hpp"
#include "histogram.hpp"

#include <mutex>

namespace duckdb {

class TempProfileCollector final : public BaseProfileCollector {
public:
	TempProfileCollector();
	~TempProfileCollector() override = default;

	void RecordOperationStart(const std::string &oper) override;
	void RecordOperationEnd(const std::string &oper) override;
	void RecordCacheAccess(CacheAccess cache_access) override;
	std::string GetProfilerType() override {
		return TEMP_PROFILE_TYPE;
	}
	void Reset() override;
	std::pair<std::string, uint64_t> GetHumanReadableStats() override;

private:
	struct OperationStats {
		// Accounted as time elapsed since unix epoch in milliseconds.
		int64_t start_timestamp = 0;
	};

	// Maps from operation name to its stats, only records ongoing operations.
	unordered_map<string, OperationStats> operation_events;
	// Only records finished operations.
	Histogram histogram;
	// Aggregated cache access condition.
	uint64_t cache_hit_count = 0;
	uint64_t cache_miss_count = 0;
	// Latest access timestamp in milliseconds since unix epoch.
	uint64_t latest_timestamp = 0;

	std::mutex stats_mutex;
};
} // namespace duckdb

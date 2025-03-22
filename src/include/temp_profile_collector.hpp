// Profile collector, which profiles and stores the latest result in memory.

#pragma once

#include "base_profile_collector.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/profiler.hpp"
#include "histogram.hpp"

#include <array>
#include <mutex>

namespace duckdb {

class TempProfileCollector final : public BaseProfileCollector {
public:
	TempProfileCollector();
	~TempProfileCollector() override = default;

	std::string GenerateOperId() const override;
	void RecordOperationStart(IoOperation io_oper, const std::string &oper) override;
	void RecordOperationEnd(IoOperation io_oper, const std::string &oper) override;
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) override;
	std::string GetProfilerType() override {
		return *TEMP_PROFILE_TYPE;
	}
	vector<CacheAccessInfo> GetCacheAccessInfo() const override;
	void Reset() override;
	std::pair<std::string, uint64_t> GetHumanReadableStats() override;

private:
	struct OperationStats {
		// Accounted as time elapsed since unix epoch in milliseconds.
		int64_t start_timestamp = 0;
	};

	using OperationStatsMap = unordered_map<string /*oper_id*/, OperationStats>;
	std::array<OperationStatsMap, kIoOperationCount> operation_events;
	// Only records finished operations, which maps from io operation to histogram.
	std::array<unique_ptr<Histogram>, kIoOperationCount> histograms;
	// Aggregated cache access condition.
	std::array<uint64_t, kCacheEntityCount * 2 /*for cache hit and miss*/> cache_access_count {};
	// Latest access timestamp in milliseconds since unix epoch.
	uint64_t latest_timestamp = 0;

	mutable std::mutex stats_mutex;
};
} // namespace duckdb

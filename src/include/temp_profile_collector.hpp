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

	LatencyGuard RecordOperationStart(IoOperation io_oper) override;
	void RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec) override;
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access, idx_t byte_count) override;
	void RecordActualCacheRead(idx_t cache_bytes, idx_t actual_bytes) override;
	std::string GetProfilerType() override {
		return *TEMP_PROFILE_TYPE;
	}
	vector<CacheAccessInfo> GetCacheAccessInfo() const override;
	void Reset() override;
	std::pair<std::string, uint64_t> GetHumanReadableStats() override;

private:
	// Only records finished operations, which maps from io operation to histogram.
	std::array<unique_ptr<Histogram>, kIoOperationCount> histograms;
	// Aggregated cache access condition.
	std::array<uint64_t, kCacheEntityCount * kCacheAccessCount> cache_access_count {};
	// Latest access timestamp in milliseconds since unix epoch.
	uint64_t latest_timestamp = 0;
	// Total number of bytes to read.
	uint64_t total_bytes_to_read = 0;
	// Total number of bytes to cache.
	uint64_t total_bytes_to_cache = 0;
	// Number of bytes served from cache (cache hits).
	uint64_t bytes_read_from_hits = 0;
	// Number of bytes fetched from remote (cache misses).
	uint64_t bytes_read_from_misses = 0;

	mutable std::mutex stats_mutex;
};
} // namespace duckdb

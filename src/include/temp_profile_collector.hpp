// Profile collector, which profiles and stores the latest result in memory.

#pragma once

#include "base_profile_collector.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/profiler.hpp"
#include "histogram.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

#include <array>

namespace duckdb {

class TempProfileCollector final : public BaseProfileCollector {
public:
	TempProfileCollector();
	~TempProfileCollector() override = default;

	LatencyGuard RecordOperationStart(IoOperation io_oper) override;
	void RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec) override;
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) override;
	void RecordActualCacheRead(idx_t cache_bytes, idx_t actual_bytes) override;
	void RecordBytesWritten(idx_t bytes) override;
	string GetProfilerType() override {
		return *TEMP_PROFILE_TYPE;
	}
	vector<CacheAccessInfo> GetCacheAccessInfo() const override;
	void Reset() override;
	std::pair<string, uint64_t> GetHumanReadableStats() override;

private:
	// Only records finished operations, which maps from io operation to histogram.
	std::array<unique_ptr<Histogram>, kIoOperationCount> histograms DUCKDB_GUARDED_BY(stats_mutex);
	// Aggregated cache access condition.
	std::array<uint64_t, kCacheEntityCount * kCacheAccessCount> cache_access_count DUCKDB_GUARDED_BY(stats_mutex) {};
	// Latest access timestamp in milliseconds since unix epoch.
	uint64_t latest_timestamp DUCKDB_GUARDED_BY(stats_mutex) = 0;
	// Total number of bytes to read.
	uint64_t total_bytes_to_read DUCKDB_GUARDED_BY(stats_mutex) = 0;
	// Total number of bytes to cache.
	uint64_t total_bytes_to_cache DUCKDB_GUARDED_BY(stats_mutex) = 0;
	// Total number of bytes written.
	uint64_t total_bytes_written DUCKDB_GUARDED_BY(stats_mutex) = 0;

	mutable concurrency::mutex stats_mutex;
};
} // namespace duckdb

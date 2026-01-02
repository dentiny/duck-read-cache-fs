// Thread-safe manager for profile collector operations.

#pragma once

#include "base_profile_collector.hpp"
#include "cache_entry_info.hpp"
#include "io_operations.hpp"

#include <mutex>

namespace duckdb {

class ProfileCollectorManager {
public:
	ProfileCollectorManager();
	~ProfileCollectorManager() = default;

	// Set the current profile collector
	void SetProfileCollector(BaseProfileCollector &profile_collector_p, const string &cache_reader_type);

	// Get the current profile collector
	// WARNING: this function returns current profiler reference, so not expected to call concurrently with set function.
	BaseProfileCollector &GetProfileCollector() const;

	// Thread-safe wrapper for profiler collector functions.
	LatencyGuard RecordOperationStart(IoOperation io_oper);
	void RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec);
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access);
	void RecordActualCacheRead(idx_t cache_size, idx_t actual_bytes);

private:
	mutable std::mutex mutex;
	// Default no-op collector used when no profile collector is set
	NoopProfileCollector noop_collector;
	// Reference to current profile collector
	BaseProfileCollector &profile_collector;
};

} // namespace duckdb


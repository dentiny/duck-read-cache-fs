// Thread-safe manager for profile collector operations.

#pragma once

#include "base_profile_collector.hpp"
#include "cache_entry_info.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "io_operations.hpp"

#include <mutex>

namespace duckdb {

class ProfileCollectorManager {
public:
	ProfileCollectorManager();
	~ProfileCollectorManager() = default;

	// Set the profile collector type based on configuration.
	void SetProfileCollectorType(const string &profile_type, const string &cache_reader_type);

	// Get the current profile collector (thread-safe).
	BaseProfileCollector &GetProfileCollector() const;

	// Thread-safe wrapper for profiler collector functions.
	LatencyGuard RecordOperationStart(IoOperation io_oper);
	void RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec);
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access);
	void RecordActualCacheRead(idx_t cache_size, idx_t actual_bytes);

private:
	mutable std::mutex mutex;
	// Owned profile collector (defaults to noop)
	unique_ptr<BaseProfileCollector> profile_collector;
};

} // namespace duckdb


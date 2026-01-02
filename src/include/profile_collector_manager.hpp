// Thread-safe manager for profile collector operations.
// All locking and thread-safety mechanisms are handled inside this class.

#pragma once

#include "base_profile_collector.hpp"
#include "cache_entry_info.hpp"
#include "io_operations.hpp"

#include <mutex>

namespace duckdb {

class ProfileCollectorManager {
public:
	ProfileCollectorManager() = default;
	~ProfileCollectorManager() = default;

	// Set the current profile collector (thread-safe).
	void SetProfileCollector(BaseProfileCollector *profile_collector_p, const string &cache_reader_type);

	// Get the current profile collector.
	BaseProfileCollector *GetProfileCollector() const;

	// Thread-safe wrapper for RecordOperationStart.
	LatencyGuard RecordOperationStart(IoOperation io_oper);

	// Thread-safe wrapper for RecordOperationEnd.
	void RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec);

	// Thread-safe wrapper for RecordCacheAccess.
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access);

	// Thread-safe wrapper for RecordActualCacheRead.
	void RecordActualCacheRead(idx_t cache_size, idx_t actual_bytes);

private:
	mutable std::mutex mutex;
	BaseProfileCollector *profile_collector = nullptr;
};

} // namespace duckdb


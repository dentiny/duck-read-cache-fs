#include "profile_collector_manager.hpp"

#include "base_profile_collector.hpp"
#include "cache_filesystem_config.hpp"
#include "temp_profile_collector.hpp"

namespace duckdb {

ProfileCollectorManager::ProfileCollectorManager() {
	// Initialize with noop collector by default
	profile_collector = make_uniq<NoopProfileCollector>();
}

void ProfileCollectorManager::SetProfileCollectorType(const string &profile_type, const string &cache_reader_type) {
	const std::lock_guard<std::mutex> lock(mutex);
	
	if (profile_type == *NOOP_PROFILE_TYPE) {
		if (profile_collector == nullptr || profile_collector->GetProfilerType() != *NOOP_PROFILE_TYPE) {
			profile_collector = make_uniq<NoopProfileCollector>();
		}
	} else if (profile_type == *TEMP_PROFILE_TYPE) {
		if (profile_collector == nullptr || profile_collector->GetProfilerType() != *TEMP_PROFILE_TYPE) {
			profile_collector = make_uniq<TempProfileCollector>();
		}
	} else {
		// Default to noop if unknown type
		if (profile_collector == nullptr) {
			profile_collector = make_uniq<NoopProfileCollector>();
		}
	}
	
	profile_collector->SetCacheReaderType(cache_reader_type);
}

BaseProfileCollector &ProfileCollectorManager::GetProfileCollector() const {
	const std::lock_guard<std::mutex> lock(mutex);
	return *profile_collector;
}

LatencyGuard ProfileCollectorManager::RecordOperationStart(IoOperation io_oper) {
	const std::lock_guard<std::mutex> lock(mutex);
	return profile_collector->RecordOperationStart(io_oper);
}

void ProfileCollectorManager::RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec) {
	const std::lock_guard<std::mutex> lock(mutex);
	profile_collector->RecordOperationEnd(io_oper, latency_millisec);
}

void ProfileCollectorManager::RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) {
	const std::lock_guard<std::mutex> lock(mutex);
	profile_collector->RecordCacheAccess(cache_entity, cache_access);
}

void ProfileCollectorManager::RecordActualCacheRead(idx_t cache_size, idx_t actual_bytes) {
	const std::lock_guard<std::mutex> lock(mutex);
	profile_collector->RecordActualCacheRead(cache_size, actual_bytes);
}

} // namespace duckdb


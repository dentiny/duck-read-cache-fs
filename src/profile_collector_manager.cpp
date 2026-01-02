#include "profile_collector_manager.hpp"

#include "base_profile_collector.hpp"

namespace duckdb {

void ProfileCollectorManager::SetProfileCollector(BaseProfileCollector *profile_collector_p,
                                                   const string &cache_reader_type) {
	const std::lock_guard<std::mutex> lock(mutex);
	profile_collector = profile_collector_p;
	if (profile_collector_p != nullptr) {
		profile_collector_p->SetCacheReaderType(cache_reader_type);
	}
}

BaseProfileCollector *ProfileCollectorManager::GetProfileCollector() const {
	const std::lock_guard<std::mutex> lock(mutex);
	return profile_collector;
}

LatencyGuard ProfileCollectorManager::RecordOperationStart(IoOperation io_oper) {
	const std::lock_guard<std::mutex> lock(mutex);
	if (profile_collector != nullptr) {
		return profile_collector->RecordOperationStart(io_oper);
	}
	// Return a no-op guard if profile collector is not set
	static NoopProfileCollector noop_collector;
	return LatencyGuard(noop_collector, io_oper);
}

void ProfileCollectorManager::RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec) {
	const std::lock_guard<std::mutex> lock(mutex);
	if (profile_collector != nullptr) {
		profile_collector->RecordOperationEnd(io_oper, latency_millisec);
	}
}

void ProfileCollectorManager::RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) {
	const std::lock_guard<std::mutex> lock(mutex);
	if (profile_collector != nullptr) {
		profile_collector->RecordCacheAccess(cache_entity, cache_access);
	}
}

void ProfileCollectorManager::RecordActualCacheRead(idx_t cache_size, idx_t actual_bytes) {
	const std::lock_guard<std::mutex> lock(mutex);
	if (profile_collector != nullptr) {
		profile_collector->RecordActualCacheRead(cache_size, actual_bytes);
	}
}

} // namespace duckdb


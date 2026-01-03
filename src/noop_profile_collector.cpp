#include "noop_profile_collector.hpp"

#include "time_utils.hpp"

namespace duckdb {

LatencyGuard NoopProfileCollector::RecordOperationStart(IoOperation io_oper) {
    latest_timestamp = GetSteadyNowMilliSecSinceEpoch();
    return LatencyGuard {*this, std::move(io_oper)};
}

vector<CacheAccessInfo> NoopProfileCollector::GetCacheAccessInfo() const {
	vector<CacheAccessInfo> cache_access_info;
	cache_access_info.resize(kCacheEntityCount);
	for (size_t idx = 0; idx < kCacheEntityCount; ++idx) {
		cache_access_info[idx].cache_type = CACHE_ENTITY_NAMES[idx];
		cache_access_info[idx].total_bytes_to_read = Value::UBIGINT(0);
		cache_access_info[idx].total_bytes_to_cache = Value::UBIGINT(0);
	}
	return cache_access_info;
}

} // namespace duckdb

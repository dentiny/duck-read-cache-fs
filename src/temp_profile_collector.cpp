#include "temp_profile_collector.hpp"
#include "utils/include/time_utils.hpp"

namespace duckdb {

void TempProfileCollector::RecordOperationStart(const std::string &oper) {
	std::lock_guard<std::mutex> lck(stats_mutex);
	const bool is_new = operation_events
	                        .emplace(oper,
	                                 OperationStats {
	                                     .start_timestamp = GetSteadyNowMilliSecSinceEpoch(),
	                                 })
	                        .second;
	D_ASSERT(is_new);
}

void TempProfileCollector::RecordOperationEnd(const std::string &oper) {
	std::lock_guard<std::mutex> lck(stats_mutex);
	auto iter = operation_events.find(oper);
	D_ASSERT(iter != operation_events.end());
	iter->second.end_timestamp = GetSteadyNowMilliSecSinceEpoch();
}

void TempProfileCollector::RecordCacheAccess(CacheAccess cache_access) {
	std::lock_guard<std::mutex> lck(stats_mutex);
	cache_hit_count += cache_access == CacheAccess::kCacheHit;
	cache_miss_count += cache_access == CacheAccess::kCacheMiss;
}

void TempProfileCollector::Reset() {
	std::lock_guard<std::mutex> lck(stats_mutex);
	operation_events.clear();
	cache_hit_count = 0;
	cache_miss_count = 0;
}

std::pair<std::string, uint64_t> TempProfileCollector::GetHumanReadableStats() {
	std::lock_guard<std::mutex> lck(stats_mutex);
	uint64_t total_time_milli = 0;
	uint64_t total_io_count = 0;
	uint64_t latest_timestamp = 0;
	for (const auto &[_, cur_stat] : operation_events) {
		// Check operation has finished.
		D_ASSERT(cur_stat.end_timestamp != 0);
		total_time_milli += cur_stat.end_timestamp - cur_stat.start_timestamp;
		++total_io_count;
		latest_timestamp = MaxValue<uint64_t>(latest_timestamp, cur_stat.end_timestamp);
	}
	double avg = static_cast<double>(total_time_milli) / total_io_count;
	auto stats = StringUtil::Format("for temp profile collector, stats for %s\n"
	                                "cache hit count = %d\n"
	                                "cache miss count = %d\n"
	                                "remote access takes %lf milliseconds in average",
	                                cache_reader_type, cache_hit_count, cache_miss_count, avg);
	return std::make_pair(std::move(stats), latest_timestamp);
}

} // namespace duckdb

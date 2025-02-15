#include "temp_profile_collector.hpp"
#include "utils/include/time_utils.hpp"

namespace duckdb {

namespace {
// Heuristic estimation of single IO request latency, out of which range are classified as outliers.
constexpr double kMinLatencyMillisec = 0;
constexpr double kMaxLatencyMillisec = 1000;
constexpr int kLatencyNumBkt = 200;
} // namespace

TempProfileCollector::TempProfileCollector() : histogram(kMinLatencyMillisec, kMaxLatencyMillisec, kLatencyNumBkt) {
}

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
	const auto now = GetSteadyNowMilliSecSinceEpoch();

	std::lock_guard<std::mutex> lck(stats_mutex);
	auto iter = operation_events.find(oper);
	D_ASSERT(iter != operation_events.end());
	histogram.Add(now - iter->second.start_timestamp);
	operation_events.erase(iter);
	latest_timestamp = now;
}

void TempProfileCollector::RecordCacheAccess(CacheAccess cache_access) {
	std::lock_guard<std::mutex> lck(stats_mutex);
	cache_hit_count += cache_access == CacheAccess::kCacheHit;
	cache_miss_count += cache_access == CacheAccess::kCacheMiss;
}

void TempProfileCollector::Reset() {
	std::lock_guard<std::mutex> lck(stats_mutex);
	histogram.Reset();
	operation_events.clear();
	cache_hit_count = 0;
	cache_miss_count = 0;
	latest_timestamp = 0;
}

std::pair<std::string, uint64_t> TempProfileCollector::GetHumanReadableStats() {
	std::lock_guard<std::mutex> lck(stats_mutex);
	auto stats = StringUtil::Format("For temp profile collector and stats for %s (unit in milliseconds)\n"
	                                "cache hit count = %d\n"
	                                "cache miss count = %d\n"
	                                "IO latency is %s",
	                                cache_reader_type, cache_hit_count, cache_miss_count, histogram.FormatString());
	return std::make_pair(std::move(stats), latest_timestamp);
}

} // namespace duckdb

#include "duckdb/common/types/uuid.hpp"
#include "temp_profile_collector.hpp"
#include "utils/include/time_utils.hpp"

namespace duckdb {

namespace {
// Heuristic estimation of single IO request latency, out of which range are classified as outliers.
constexpr double MIN_LATENCY_MILLISEC = 0;
constexpr double MAX_LATENCY_MILLISEC = 1000;
constexpr int LATENCY_NUM_BKT = 200;

const string LATENCY_HISTOGRAM_ITEM = "latency";
const string LATENCY_HISTOGRAM_UNIT = "millisec";
} // namespace

TempProfileCollector::TempProfileCollector() : histogram(MIN_LATENCY_MILLISEC, MAX_LATENCY_MILLISEC, LATENCY_NUM_BKT) {
	histogram.SetStatsDistribution(LATENCY_HISTOGRAM_ITEM, LATENCY_HISTOGRAM_UNIT);
}

std::string TempProfileCollector::GetOperId() const {
	return UUID::ToString(UUID::GenerateRandomUUID());
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

void TempProfileCollector::RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) {
	std::lock_guard<std::mutex> lck(stats_mutex);
	const size_t arr_idx = static_cast<size_t>(cache_entity) * 2 + static_cast<size_t>(cache_access);
	++cache_access_count[arr_idx];
}

void TempProfileCollector::Reset() {
	std::lock_guard<std::mutex> lck(stats_mutex);
	histogram.Reset();
	operation_events.clear();
	cache_access_count.fill(0);
	latest_timestamp = 0;
}

std::pair<std::string, uint64_t> TempProfileCollector::GetHumanReadableStats() {
	std::lock_guard<std::mutex> lck(stats_mutex);
	auto stats = StringUtil::Format("For temp profile collector and stats for %s (unit in milliseconds)\n"
	                                "metadata cache hit count = %d\n"
	                                "metadata cache miss count = %d\n"
	                                "data block cache hit count = %d\n"
	                                "data block cache miss count = %d\n"
	                                "IO latency is %s",
	                                cache_reader_type, cache_access_count[0], cache_access_count[1],
	                                cache_access_count[2], cache_access_count[3], histogram.FormatString());
	return std::make_pair(std::move(stats), latest_timestamp);
}

} // namespace duckdb

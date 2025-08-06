#include "temp_profile_collector.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "utils/include/no_destructor.hpp"
#include "utils/include/time_utils.hpp"

namespace duckdb {

namespace {
// Heuristic estimation of single IO request latency, out of which range are classified as outliers.
constexpr double MIN_READ_LATENCY_MILLISEC = 0;
constexpr double MAX_READ_LATENCY_MILLISEC = 1000;
constexpr int READ_LATENCY_NUM_BKT = 100;

constexpr double MIN_OPEN_LATENCY_MILLISEC = 0;
constexpr double MAX_OPEN_LATENCY_MILLISEC = 1000;
constexpr int OPEN_LATENCY_NUM_BKT = 100;

constexpr double MIN_GLOB_LATENCY_MILLISEC = 0;
constexpr double MAX_GLOB_LATENCY_MILLISEC = 1000;
constexpr int GLOB_LATENCY_NUM_BKT = 100;

const NoDestructor<string> LATENCY_HISTOGRAM_ITEM {"latency"};
const NoDestructor<string> LATENCY_HISTOGRAM_UNIT {"millisec"};
} // namespace

TempProfileCollector::TempProfileCollector() {
	histograms[static_cast<idx_t>(IoOperation::kRead)] =
	    make_uniq<Histogram>(MIN_READ_LATENCY_MILLISEC, MAX_READ_LATENCY_MILLISEC, READ_LATENCY_NUM_BKT);
	histograms[static_cast<idx_t>(IoOperation::kRead)]->SetStatsDistribution(*LATENCY_HISTOGRAM_ITEM,
	                                                                         *LATENCY_HISTOGRAM_UNIT);
	operation_events[static_cast<idx_t>(IoOperation::kRead)] = OperationStatsMap {};

	histograms[static_cast<idx_t>(IoOperation::kOpen)] =
	    make_uniq<Histogram>(MIN_OPEN_LATENCY_MILLISEC, MAX_OPEN_LATENCY_MILLISEC, OPEN_LATENCY_NUM_BKT);
	histograms[static_cast<idx_t>(IoOperation::kOpen)]->SetStatsDistribution(*LATENCY_HISTOGRAM_ITEM,
	                                                                         *LATENCY_HISTOGRAM_UNIT);
	operation_events[static_cast<idx_t>(IoOperation::kOpen)] = OperationStatsMap {};

	histograms[static_cast<idx_t>(IoOperation::kGlob)] =
	    make_uniq<Histogram>(MIN_GLOB_LATENCY_MILLISEC, MAX_GLOB_LATENCY_MILLISEC, GLOB_LATENCY_NUM_BKT);
	histograms[static_cast<idx_t>(IoOperation::kGlob)]->SetStatsDistribution(*LATENCY_HISTOGRAM_ITEM,
	                                                                         *LATENCY_HISTOGRAM_UNIT);
	operation_events[static_cast<idx_t>(IoOperation::kGlob)] = OperationStatsMap {};
}

std::string TempProfileCollector::GenerateOperId() const {
	return UUID::ToString(UUID::GenerateRandomUUID());
}

void TempProfileCollector::RecordOperationStart(IoOperation io_oper, const std::string &oper_id) {
	std::lock_guard<std::mutex> lck(stats_mutex);
	auto &cur_oper_event = operation_events[static_cast<idx_t>(io_oper)];
	const bool is_new = cur_oper_event
	                        .emplace(oper_id,
	                                 OperationStats {
	                                     .start_timestamp = GetSteadyNowMilliSecSinceEpoch(),
	                                 })
	                        .second;
	D_ASSERT(is_new);
}

void TempProfileCollector::RecordOperationEnd(IoOperation io_oper, const std::string &oper_id) {
	const auto now = GetSteadyNowMilliSecSinceEpoch();

	std::lock_guard<std::mutex> lck(stats_mutex);
	auto &cur_oper_event = operation_events[static_cast<idx_t>(io_oper)];
	auto iter = cur_oper_event.find(oper_id);
	D_ASSERT(iter != cur_oper_event.end());

	auto &cur_histogram = histograms[static_cast<idx_t>(io_oper)];
	cur_histogram->Add(now - iter->second.start_timestamp);
	cur_oper_event.erase(iter);
	latest_timestamp = now;
}

void TempProfileCollector::RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) {
	std::lock_guard<std::mutex> lck(stats_mutex);
	const size_t arr_idx = static_cast<size_t>(cache_entity) * kCacheAccessCount + static_cast<size_t>(cache_access);
	++cache_access_count[arr_idx];
}

void TempProfileCollector::Reset() {
	std::lock_guard<std::mutex> lck(stats_mutex);
	for (auto &cur_oper_event : operation_events) {
		cur_oper_event.clear();
	}
	for (auto &cur_histogram : histograms) {
		cur_histogram->Reset();
	}
	cache_access_count.fill(0);
	latest_timestamp = 0;
}

vector<CacheAccessInfo> TempProfileCollector::GetCacheAccessInfo() const {
	std::lock_guard<std::mutex> lck(stats_mutex);
	vector<CacheAccessInfo> cache_access_info;
	cache_access_info.reserve(kCacheEntityCount);
	for (idx_t idx = 0; idx < kCacheEntityCount; ++idx) {
		cache_access_info.emplace_back(CacheAccessInfo {
		    .cache_type = CACHE_ENTITY_NAMES[idx],
		    .cache_hit_count = cache_access_count[idx * kCacheAccessCount],
		    .cache_miss_count = cache_access_count[idx * kCacheAccessCount + 1],
			.cache_miss_by_in_use = cache_access_count[idx * kCacheAccessCount + 2],
		});
	}
	return cache_access_info;
}

std::pair<std::string, uint64_t> TempProfileCollector::GetHumanReadableStats() {
	std::lock_guard<std::mutex> lck(stats_mutex);

	string stats =
	    StringUtil::Format("For temp profile collector and stats for %s (unit in milliseconds)\n", cache_reader_type);

	// Record cache miss and cache hit count.
	for (idx_t cur_entity_idx = 0; cur_entity_idx < kCacheEntityCount; ++cur_entity_idx) {
		stats = StringUtil::Format("%s\n"
		                           "%s cache hit count = %d\n"
		                           "%s cache miss count = %d\n",
								   "%s cache miss by in-use resource = %d\n",
		                           stats, 
								   CACHE_ENTITY_NAMES[cur_entity_idx], 
								   cache_access_count[cur_entity_idx * kCacheAccessCount],
		                           CACHE_ENTITY_NAMES[cur_entity_idx], 
								   cache_access_count[cur_entity_idx * kCacheAccessCount + 1],
								   CACHE_ENTITY_NAMES[cur_entity_idx], 
								   CACHE_ENTITY_NAMES[cur_entity_idx * kCacheAccessCount + 2]);
	}

	// Record IO operation latency.
	for (idx_t cur_oper_idx = 0; cur_oper_idx < kIoOperationCount; ++cur_oper_idx) {
		const auto &cur_histogram = histograms[cur_oper_idx];
		if (cur_histogram->counts() == 0) {
			continue;
		}
		stats = StringUtil::Format("%s\n"
		                           "%s operation latency is %s",
		                           stats, OPER_NAMES[cur_oper_idx], cur_histogram->FormatString());
	}

	return std::make_pair(std::move(stats), latest_timestamp);
}

} // namespace duckdb

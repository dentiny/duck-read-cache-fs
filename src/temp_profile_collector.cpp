#include "temp_profile_collector.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "utils/include/no_destructor.hpp"
#include "utils/include/time_utils.hpp"

namespace duckdb {

namespace {
// Heuristic estimation of single IO request latency, out of which range are classified as outliers.
// Heuristic estimation of single IO request latency, out of which range are classified as outliers.
struct LatencyHeuristic {
	double min_latency_ms;
	double max_latency_ms;
	int num_buckets;
};

inline constexpr std::array<LatencyHeuristic, kIoOperationCount> kLatencyHeuristics = {{
    // Read
    {0, 1000, 100},
    // Open
    {0, 1000, 100},
    // Glob.
    {0, 1000, 100},
    // Disk cache read
    {0, 500, 100},
}};

const NoDestructor<string> LATENCY_HISTOGRAM_ITEM {"latency"};
const NoDestructor<string> LATENCY_HISTOGRAM_UNIT {"millisec"};
} // namespace

TempProfileCollector::TempProfileCollector() {
	for (idx_t idx = 0; idx < kIoOperationCount; ++idx) {
		histograms[idx] =
		    make_uniq<Histogram>(kLatencyHeuristics[idx].min_latency_ms, kLatencyHeuristics[idx].max_latency_ms,
		                         kLatencyHeuristics[idx].num_buckets);
		histograms[idx]->SetStatsDistribution(*LATENCY_HISTOGRAM_ITEM, *LATENCY_HISTOGRAM_UNIT);
	}
}

LatencyGuard TempProfileCollector::RecordOperationStart(IoOperation io_oper) {
	return LatencyGuard {*this, std::move(io_oper)};
	const std::lock_guard<std::mutex> lck(stats_mutex);
}

void TempProfileCollector::RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec) {
	const auto now = GetSteadyNowMilliSecSinceEpoch();

	const std::lock_guard<std::mutex> lck(stats_mutex);
	auto &cur_histogram = histograms[static_cast<idx_t>(io_oper)];
	cur_histogram->Add(latency_millisec);
	latest_timestamp = now;
}

void TempProfileCollector::RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access, idx_t byte_count) {
	const std::lock_guard<std::mutex> lck(stats_mutex);
	const size_t arr_idx = static_cast<size_t>(cache_entity) * kCacheAccessCount + static_cast<size_t>(cache_access);
	++cache_access_count[arr_idx];

	// Track byte-level statistics for data cache
	if (cache_entity == CacheEntity::kData) {
		if (cache_access == CacheAccess::kCacheHit) {
			bytes_read_from_hits += byte_count;
		} else if (cache_access == CacheAccess::kCacheMiss) {
			bytes_read_from_misses += byte_count;
		}
	}
}

void TempProfileCollector::RecordActualCacheRead(idx_t cache_bytes, idx_t actual_bytes) {
	const std::lock_guard<std::mutex> lck(stats_mutex);
	total_bytes_to_read += actual_bytes;
	total_bytes_to_cache += cache_bytes;
}

void TempProfileCollector::Reset() {
	const std::lock_guard<std::mutex> lck(stats_mutex);
	for (auto &cur_histogram : histograms) {
		cur_histogram->Reset();
	}
	cache_access_count.fill(0);
	total_bytes_to_cache = 0;
	total_bytes_to_read = 0;
	bytes_read_from_hits = 0;
	bytes_read_from_misses = 0;
	latest_timestamp = 0;
}

vector<CacheAccessInfo> TempProfileCollector::GetCacheAccessInfo() const {
	const std::lock_guard<std::mutex> lck(stats_mutex);
	vector<CacheAccessInfo> cache_access_info;
	cache_access_info.reserve(kCacheEntityCount);
	for (idx_t idx = 0; idx < kCacheEntityCount; ++idx) {
		cache_access_info.emplace_back(CacheAccessInfo {
		    .cache_type = CACHE_ENTITY_NAMES[idx],
		    .cache_hit_count = cache_access_count[idx * kCacheAccessCount],
		    .cache_miss_count = cache_access_count[idx * kCacheAccessCount + 1],
		    .cache_miss_by_in_use = cache_access_count[idx * kCacheAccessCount + 2],
		});

		// Record "bytes to read" and "bytes to cache" for data cache.
		if (idx == static_cast<idx_t>(IoOperation::kRead)) {
			cache_access_info[idx].total_bytes_to_cache = Value::UBIGINT(total_bytes_to_cache);
			cache_access_info[idx].total_bytes_to_read = Value::UBIGINT(total_bytes_to_read);
			cache_access_info[idx].bytes_read_from_hits = Value::UBIGINT(bytes_read_from_hits);
			cache_access_info[idx].bytes_read_from_misses = Value::UBIGINT(bytes_read_from_misses);
		}
	}
	return cache_access_info;
}

std::pair<std::string, uint64_t> TempProfileCollector::GetHumanReadableStats() {
	const std::lock_guard<std::mutex> lck(stats_mutex);

	string stats =
	    StringUtil::Format("For temp profile collector and stats for %s (unit in milliseconds)\n", cache_reader_type);

	// Record cache miss and cache hit count.
	for (idx_t cur_entity_idx = 0; cur_entity_idx < kCacheEntityCount; ++cur_entity_idx) {
		stats = StringUtil::Format(
		    "%s\n"
		    "%s cache hit count = %d\n"
		    "%s cache miss count = %d\n"
		    "%s cache miss by in-use resource = %d\n",
		    stats, CACHE_ENTITY_NAMES[cur_entity_idx], cache_access_count[cur_entity_idx * kCacheAccessCount],
		    CACHE_ENTITY_NAMES[cur_entity_idx], cache_access_count[cur_entity_idx * kCacheAccessCount + 1],
		    CACHE_ENTITY_NAMES[cur_entity_idx], cache_access_count[cur_entity_idx * kCacheAccessCount + 2]);

		// Record "bytes to read" and "bytes to cache" for data cache.
		if (cur_entity_idx == static_cast<idx_t>(IoOperation::kRead)) {
			stats = StringUtil::Format("%s\n"
			                           "for data cache, "
			                           "number of bytes to read = %d\n"
			                           "number of bytes to cache = %d\n"
			                           "bytes read from hits = %d\n"
			                           "bytes read from misses = %d\n",
			                           stats, total_bytes_to_read, total_bytes_to_cache, bytes_read_from_hits,
			                           bytes_read_from_misses);
		}
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

#include "cache_exclusion_manager.hpp"

#include <utility>

#include "duckdb/common/helper.hpp"

namespace duckdb {

void CacheExclusionManager::AddExclusionRegex(const string &regex) {
	auto pattern = make_uniq<::duckdb_re2::RE2>(regex);
	std::lock_guard<std::mutex> lck(mu);
	exclusion_regexes.emplace_back(std::move(pattern));
}

void CacheExclusionManager::ResetExclusionRegex() {
	std::lock_guard<std::mutex> lck(mu);
	exclusion_regexes.clear();
}

bool CacheExclusionManager::MatchAnyExclusion(const string &filepath) const {
	// TODO(hjiang): Could be accessed by multiple threads and potentially be a bottleneck, could use shared pointer to
	// improve.
	std::lock_guard<std::mutex> lck(mu);
	for (const auto &cur_pattern : exclusion_regexes) {
		if (RE2::PartialMatch(filepath, *cur_pattern)) {
			return true;
		}
	}
	return false;
}

vector<string> CacheExclusionManager::GetExclusionRegex() const {
	vector<string> results;
	std::lock_guard<std::mutex> lck(mu);
	results.reserve(exclusion_regexes.size());
	for (const auto &cur_pattern : exclusion_regexes) {
		results.emplace_back(cur_pattern->pattern());
	}
	return results;
}

} // namespace duckdb

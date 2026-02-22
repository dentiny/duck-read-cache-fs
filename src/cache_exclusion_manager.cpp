#include "cache_exclusion_manager.hpp"

namespace duckdb {

void CacheExclusionManager::AddExclusionRegex(const string &regex) {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	exclusion_regexes.emplace_back(regex);
	exclusion_patterns.emplace_back(regex);
}

void CacheExclusionManager::ResetExclusionRegex() {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	exclusion_regexes.clear();
	exclusion_patterns.clear();
}

bool CacheExclusionManager::MatchAnyExclusion(const string &filepath) const {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	for (const auto &cur_pattern : exclusion_regexes) {
		if (std::regex_search(filepath, cur_pattern)) {
			return true;
		}
	}
	return false;
}

vector<string> CacheExclusionManager::GetExclusionRegex() const {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	return exclusion_patterns;
}

} // namespace duckdb

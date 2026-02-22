// A class to manage cache exclusion list.

#pragma once

#include <regex>

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

namespace duckdb {

class CacheExclusionManager {
public:
	// Add exclusion regex.
	void AddExclusionRegex(const string &regex);

	// Reset exclusion regex.
	void ResetExclusionRegex();

	// Whether the given filepath matches ANY of the regex to exclude cache.
	bool MatchAnyExclusion(const string &filepath) const;

	// Get all excluded cache regex in string format.
	vector<string> GetExclusionRegex() const;

private:
	mutable concurrency::mutex mu;
	vector<std::regex> exclusion_regexes DUCKDB_GUARDED_BY(mu);
	vector<string> exclusion_patterns DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb

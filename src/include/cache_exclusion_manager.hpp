// A class to manage cache exclusion list.

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "mutex.hpp"
#include "re2/re2.h"
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
	vector<unique_ptr<::duckdb_re2::RE2>> exclusion_regexes DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb

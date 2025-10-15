// A class to manage cache exclusion list.

#pragma once

#include <mutex>

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "re2/re2.h"

namespace duckdb {

class CacheExclusionManager {
public:
	static CacheExclusionManager &GetInstance();

	// Add exclusion regex.
	void AddExlusionRegex(string regex);

	// Reset exclusion regex.
	void ResetExclusionRegex();

	// Whether the given filepath matches ANY of the regex to exclude cache.
	bool MatchAnyExclusion(const string &filepath) const;

	// Get all excluded cache regex in string format.
	vector<string> GetExclusionRegex() const;

private:
	CacheExclusionManager() = default;

	mutable std::mutex mu;
	vector<unique_ptr<::duckdb_re2::RE2>> exclusion_regexes;
};

} // namespace duckdb

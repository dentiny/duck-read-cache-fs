#pragma once

#include <cstdint>
#include <string>

namespace duckdb {

struct CacheEntryInfo {
	std::string cache_filepath;
	std::string remote_filename;
	uint64_t start_offset = 0; // Inclusive.
	uint64_t end_offset = 0;   // Exclusive.
	std::string cache_type;    // Either in-memory or on-disk.
};

bool operator<(const CacheEntryInfo &lhs, const CacheEntryInfo &rhs);

} // namespace duckdb

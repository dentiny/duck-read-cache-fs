#pragma once

#include <cstdint>
#include <string>

namespace duckdb {

// Entry information for data cache, which applies to both in-memory cache and on-disk cache.
struct DataCacheEntryInfo {
	std::string cache_filepath;
	std::string remote_filename;
	uint64_t start_offset = 0; // Inclusive.
	uint64_t end_offset = 0;   // Exclusive.
	std::string cache_type;    // Either in-memory or on-disk.
};

bool operator<(const DataCacheEntryInfo &lhs, const DataCacheEntryInfo &rhs);

// Cache access information, which applies to metadata and file handle cache.
struct CacheAccessInfo {
	std::string cache_type;
	uint64_t cache_hit_count = 0;
	uint64_t cache_miss_count = 0;
};

bool operator<(const CacheAccessInfo &lhs, const CacheAccessInfo &rhs);

} // namespace duckdb

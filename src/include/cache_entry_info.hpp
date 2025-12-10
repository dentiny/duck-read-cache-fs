#pragma once

#include <cstdint>

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

// Entry information for data cache, which applies to both in-memory cache and on-disk cache.
struct DataCacheEntryInfo {
	string cache_filepath;
	string remote_filename;
	uint64_t start_offset = 0; // Inclusive.
	uint64_t end_offset = 0;   // Exclusive.
	string cache_type;         // Either in-memory or on-disk.
};

bool operator<(const DataCacheEntryInfo &lhs, const DataCacheEntryInfo &rhs);

// Cache access information, which applies to metadata and file handle cache.
struct CacheAccessInfo {
	// Cache entity name.
	string cache_type;
	// Number of cache hit.
	uint64_t cache_hit_count = 0;
	// Number of cache miss.
	uint64_t cache_miss_count = 0;
	// Number of cache miss, caused by in-use exclusive resource.
	// Only useful for exclusive resource, used to indicate whether cache miss is caused by small cache size, or low
	// cache hit ratio.
	uint64_t cache_miss_by_in_use = 0;
	// Only made for data cache.
	// Record number of bytes to read.
	Value total_bytes_to_read = Value {};
	// Only made for daya cache.
	// Record number of bytes to cache.
	Value total_bytes_to_cache = Value {};
	// Only made for data cache.
	// Number of bytes served from cache (cache hits).
	Value bytes_read_from_hits = Value {};
	// Only made for data cache.
	// Number of bytes fetched from remote (cache misses).
	Value bytes_read_from_misses = Value {};
};

bool operator<(const CacheAccessInfo &lhs, const CacheAccessInfo &rhs);

} // namespace duckdb

#pragma once

#include <string>
#include <cstdint>

#include "size_literals.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Default configuration
//===--------------------------------------------------------------------===//
inline const idx_t DEFAULT_CACHE_BLOCK_SIZE = 64_KiB;
inline const std::string DEFAULT_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_cached_http_cache";

// To prevent go out of disk space, we set a threshold to disable local caching
// if insufficient.
inline const idx_t MIN_DISK_SPACE_FOR_CACHE = 1_MiB;

// Maximum in-memory cache block number, which caps the overall memory
// consumption as (block size * max block count).
inline const idx_t DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT = 256;

// Number of seconds which we define as the threshold of staleness.
inline constexpr idx_t CACHE_FILE_STALENESS_SECOND = 24 * 3600; // 1 day

//===--------------------------------------------------------------------===//
// Global configuration
//===--------------------------------------------------------------------===//
inline idx_t g_cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
inline std::string g_on_disk_cache_directory = DEFAULT_ON_DISK_CACHE_DIRECTORY;
inline idx_t g_max_in_mem_cache_block_count = DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT;

//===--------------------------------------------------------------------===//
// Configuration struct
//===--------------------------------------------------------------------===//
struct OnDiskCacheConfig {
	// Cache block size.
	idx_t block_size = g_cache_block_size;
	// Cache storage location on local filesystem.
	std::string on_disk_cache_directory = g_on_disk_cache_directory;
};

struct InMemoryCacheConfig {
	// Cache block size.
	idx_t block_size = g_cache_block_size;
	// Max cache size.
	idx_t block_count = g_max_in_mem_cache_block_count;
};

} // namespace duckdb

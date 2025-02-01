#pragma once

#include <string>
#include <cstdint>

#include "size_literals.hpp"

namespace duckdb {

// TODO(hjiang): Hard code block size for now, in the future we should allow
// global configuration via `SET GLOBAL`.
inline const uint64_t DEFAULT_BLOCK_SIZE = 64_KiB;
inline const std::string ON_DISK_CACHE_DIRECTORY =
    "/tmp/duckdb_cached_http_cache";

// To prevent go out of disk space, we set a threshold to disable local caching
// if insufficient.
inline const uint64_t MIN_DISK_SPACE_FOR_CACHE = 1_MiB;

// Maximum in-memory cache block number, which caps the overall memory
// consumption as (block size * max block count).
inline const uint64_t MAX_IN_MEM_CACHE_BLOCK_COUNT = 256;

// Number of seconds which we define as the threshold of staleness.
inline constexpr uint64_t CACHE_FILE_STALENESS_SECOND = 24 * 3600; // 1 day

// TODO(hjiang): Add constraint on largest thread number.
struct OnDiskCacheConfig {
  // Cache block size.
  uint64_t block_size = DEFAULT_BLOCK_SIZE;
  // Cache storage location on local filesystem.
  std::string on_disk_cache_directory = ON_DISK_CACHE_DIRECTORY;
};

struct InMemoryCacheConfig {
  // Cache block size.
  uint64_t block_size = DEFAULT_BLOCK_SIZE;
  // Max cache size.
  uint64_t block_count = MAX_IN_MEM_CACHE_BLOCK_COUNT;
};

} // namespace duckdb

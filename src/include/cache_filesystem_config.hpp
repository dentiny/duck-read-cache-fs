#pragma once

#include <string>
#include <cstdint>

namespace duckdb {

// TODO(hjiang): Hard code block size for now, in the future we should allow
// global configuration via `SET GLOBAL`.
inline constexpr uint64_t DEFAULT_BLOCK_SIZE = 64ULL * 1024;
inline const std::string ON_DISK_CACHE_DIRECTORY =
    "/tmp/duckdb_cached_http_cache";

// To prevent go out of disk space, we set a threshold to disable local caching
// if insufficient.
inline constexpr uint64_t MIN_DISK_SPACE_FOR_CACHE = 1024ULL * 1024;

// Number of seconds which we define as the threshold of staleness.
inline constexpr uint64_t CACHE_FILE_STALENESS_SECOND = 24 * 3600; // 1 day

// TODO(hjiang): Add constraint on largest thread number.
struct OnDiskCacheConfig {
  // Cache block size.
  uint64_t block_size = DEFAULT_BLOCK_SIZE;
  // Cache storage location on local filesystem.
  std::string on_disk_cache_directory = ON_DISK_CACHE_DIRECTORY;
};

} // namespace duckdb

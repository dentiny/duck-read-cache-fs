#pragma once

#include <string>
#include <cstdint>

namespace duckdb {

// TODO(hjiang): Hard code block size for now, in the future we should allow
// global configuration via `SET GLOBAL`.
inline constexpr uint64_t DEFAULT_BLOCK_SIZE = 5000;
inline const std::string ON_DISK_CACHE_DIRECTORY =
    "/tmp/duckdb_cached_http_cache";

// To prevent go out of disk space, we set a threshold to disable local caching
// if insufficient.
inline constexpr uint64_t MIN_DISK_SPACE_FOR_CACHE = 1024ULL * 1024 * 1024;

} // namespace duckdb

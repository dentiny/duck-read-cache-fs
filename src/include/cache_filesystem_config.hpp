#pragma once

#include <string>
#include <cstdint>

namespace duckdb {

// TODO(hjiang): Hard code block size for now, in the future we should allow
// global configuration via `SET GLOBAL`.
inline uint64_t DEFAULT_BLOCK_SIZE = 5000;
inline std::string ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_cached_http_cache";

} // namespace duckdb

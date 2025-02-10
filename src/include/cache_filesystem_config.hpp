#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/typedefs.hpp"
#include "size_literals.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Config constant
//===--------------------------------------------------------------------===//
inline const std::string ON_DISK_CACHE_TYPE = "on_disk";
inline const std::string IN_MEM_CACHE_TYPE = "in_mem";

//===--------------------------------------------------------------------===//
// Default configuration
//===--------------------------------------------------------------------===//
inline const idx_t DEFAULT_CACHE_BLOCK_SIZE = 64_KiB;
inline const std::string DEFAULT_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_cached_http_cache";

// Default to use on-disk cache filesystem.
inline std::string DEFAULT_CACHE_TYPE = ON_DISK_CACHE_TYPE;

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
inline std::string g_cache_type = DEFAULT_CACHE_TYPE;

// Used for testing purpose, which has a higher priority over [g_cache_type], and won't be reset.
// TODO(hjiang): A better is bake configuration into `FileOpener`.
inline std::string g_test_cache_type = "";

//===--------------------------------------------------------------------===//
// Util function for filesystem configurations.
//===--------------------------------------------------------------------===//

// Set global cache filesystem configuration.
void SetGlobalConfig(optional_ptr<FileOpener> opener);

// Reset all global cache filesystem configuration.
void ResetGlobalConfig();

} // namespace duckdb

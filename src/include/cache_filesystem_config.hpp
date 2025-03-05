#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/typedefs.hpp"
#include "size_literals.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Config constant
//===--------------------------------------------------------------------===//
inline const std::string NOOP_CACHE_TYPE = "noop";
inline const std::string ON_DISK_CACHE_TYPE = "on_disk";
inline const std::string IN_MEM_CACHE_TYPE = "in_mem";
inline const std::unordered_set<std::string> ALL_CACHE_TYPES = {NOOP_CACHE_TYPE, ON_DISK_CACHE_TYPE, IN_MEM_CACHE_TYPE};

// Default profile option, which performs no-op.
inline const std::string NOOP_PROFILE_TYPE = "noop";
// Store the latest IO operation profiling result, which potentially suffers concurrent updates.
inline const std::string TEMP_PROFILE_TYPE = "temp";
// Store the IO operation profiling results into duckdb table, which unblocks advanced analysis.
inline const std::string PERSISTENT_PROFILE_TYPE = "duckdb";
inline const std::unordered_set<std::string> ALL_PROFILE_TYPES = {NOOP_PROFILE_TYPE, TEMP_PROFILE_TYPE,
                                                                  PERSISTENT_PROFILE_TYPE};

//===--------------------------------------------------------------------===//
// Default configuration
//===--------------------------------------------------------------------===//
inline const idx_t DEFAULT_CACHE_BLOCK_SIZE = 64_KiB;
inline const std::string DEFAULT_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_cache_httpfs_cache";

// Default to use on-disk cache filesystem.
inline std::string DEFAULT_CACHE_TYPE = ON_DISK_CACHE_TYPE;

// To prevent go out of disk space, we set a threshold to disallow local caching if insufficient. It applies to all
// filesystems. The value here is the decimal representation for percentage value; for example, 0.05 means 5%.
inline const double MIN_DISK_SPACE_PERCENTAGE_FOR_CACHE = 0.05;

// Maximum in-memory cache block number, which caps the overall memory consumption as (block size * max block count).
inline const idx_t DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT = 256;

// Number of seconds which we define as the threshold of staleness.
inline constexpr idx_t CACHE_FILE_STALENESS_SECOND = 24 * 3600; // 1 day

// Default option for profile type.
inline std::string DEFAULT_PROFILE_TYPE = NOOP_PROFILE_TYPE;

// Default max number of parallel subrequest for a single filesystem read request. 0 means no limit.
inline uint64_t DEFAULT_MAX_SUBREQUEST_COUNT = 0;

// Default enable metadata cache.
inline bool DEFAULT_ENABLE_METADATA_CACHE = true;

// Default not ignore SIGPIPE in the extension.
inline bool DEFAULT_IGNORE_SIGPIPE = false;

// Default min disk bytes required for on-disk cache; by default 0 which user doesn't specify and override, and default
// value will be considered.
inline idx_t DEFAULT_MIN_DISK_BYTES_FOR_CACHE = 0;

//===--------------------------------------------------------------------===//
// Global configuration
//===--------------------------------------------------------------------===//
inline idx_t g_cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
inline std::string g_on_disk_cache_directory = DEFAULT_ON_DISK_CACHE_DIRECTORY;
inline idx_t g_max_in_mem_cache_block_count = DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT;
inline std::string g_cache_type = DEFAULT_CACHE_TYPE;
inline std::string g_profile_type = DEFAULT_PROFILE_TYPE;
inline uint64_t g_max_subrequest_count = DEFAULT_MAX_SUBREQUEST_COUNT;
inline bool g_enable_metadata_cache = DEFAULT_ENABLE_METADATA_CACHE;
inline bool g_ignore_sigpipe = DEFAULT_IGNORE_SIGPIPE;
inline idx_t g_min_disk_bytes_for_cache = DEFAULT_MIN_DISK_BYTES_FOR_CACHE;

// Used for testing purpose, which has a higher priority over [g_cache_type], and won't be reset.
// TODO(hjiang): A better is bake configuration into `FileOpener`.
inline std::string g_test_cache_type = "";

// Used for testing purpose, which disable on-disk cache if true.
inline bool g_test_insufficient_disk_space = false;

//===--------------------------------------------------------------------===//
// Util function for filesystem configurations.
//===--------------------------------------------------------------------===//

// Set global cache filesystem configuration.
void SetGlobalConfig(optional_ptr<FileOpener> opener);

// Reset all global cache filesystem configuration.
void ResetGlobalConfig();

// Get concurrent IO sub-request count.
uint64_t GetThreadCountForSubrequests(uint64_t io_request_count);

} // namespace duckdb

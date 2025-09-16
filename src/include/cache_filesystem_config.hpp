#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "no_destructor.hpp"
#include "size_literals.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Config constant
//===--------------------------------------------------------------------===//
inline const NoDestructor<string> NOOP_CACHE_TYPE {"noop"};
inline const NoDestructor<string> ON_DISK_CACHE_TYPE {"on_disk"};
inline const NoDestructor<string> IN_MEM_CACHE_TYPE {"in_mem"};
inline const std::unordered_set<string> ALL_CACHE_TYPES {*NOOP_CACHE_TYPE, *ON_DISK_CACHE_TYPE,
                                                              *IN_MEM_CACHE_TYPE};

// Creation timestamp-based on-disk eviction policy.
inline const NoDestructor<string> ON_DISK_CREATION_TIMESTAMP_EVICTION {"creation_timestamp"};
// On-disk LRU eviction policy made for single-process usage.
inline const NoDestructor<string> ON_DISK_LRU_SINGLE_PROC_EVICTION {"lru_sp"};

// Default profile option, which performs no-op.
inline const NoDestructor<string> NOOP_PROFILE_TYPE {"noop"};
// Store the latest IO operation profiling result, which potentially suffers concurrent updates.
inline const NoDestructor<string> TEMP_PROFILE_TYPE {"temp"};
// Store the IO operation profiling results into duckdb table, which unblocks advanced analysis.
inline const NoDestructor<string> PERSISTENT_PROFILE_TYPE {"duckdb"};
inline const NoDestructor<std::unordered_set<string>> ALL_PROFILE_TYPES {*NOOP_PROFILE_TYPE, *TEMP_PROFILE_TYPE,
                                                                              *PERSISTENT_PROFILE_TYPE};

//===--------------------------------------------------------------------===//
// Default configuration
//===--------------------------------------------------------------------===//
inline const idx_t DEFAULT_CACHE_BLOCK_SIZE = 512_KiB;
inline const NoDestructor<string> DEFAULT_ON_DISK_CACHE_DIRECTORY {"/tmp/duckdb_cache_httpfs_cache"};

// Default to use on-disk cache filesystem.
inline NoDestructor<string> DEFAULT_CACHE_TYPE {*ON_DISK_CACHE_TYPE};

// Default to timestamp-based on-disk cache eviction policy.
inline NoDestructor<string> DEFAULT_ON_DISK_EVICTION_POLICY {*ON_DISK_CREATION_TIMESTAMP_EVICTION};

// To prevent go out of disk space, we set a threshold to disallow local caching if insufficient. It applies to all
// filesystems. The value here is the decimal representation for percentage value; for example, 0.05 means 5%.
inline constexpr double MIN_DISK_SPACE_PERCENTAGE_FOR_CACHE = 0.05;

// Maximum in-memory cache block number, which caps the overall memory consumption as (block size * max block count).
inline constexpr idx_t DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT = 256;

// Default timeout in seconds for in-memory block cache entries.
inline constexpr idx_t DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC = 3600ULL * 1000 /*1hour*/;

// Max number of cache entries for file metadata cache.
inline static constexpr size_t DEFAULT_MAX_METADATA_CACHE_ENTRY = 250;

// Timeout in milliseconds of cache entries for file metadata cache.
inline static constexpr uint64_t DEFAULT_METADATA_CACHE_ENTRY_TIMEOUT_MILLISEC = 3600ULL * 1000 /*1hour*/;

// Number of seconds which we define as the threshold of staleness for metadata entries.
inline constexpr idx_t CACHE_FILE_STALENESS_SECOND = 24 * 3600; // 1 day

// Max number of cache entries for file handle cache.
inline static constexpr size_t DEFAULT_MAX_FILE_HANDLE_CACHE_ENTRY = 250;

// Timeout in milliseconds of cache entries for file handle cache.
inline static constexpr uint64_t DEFAULT_FILE_HANDLE_CACHE_ENTRY_TIMEOUT_MILLISEC = 3600ULL * 1000 /*1hour*/;

// Max number of cache entries for glob cache.
inline static constexpr size_t DEFAULT_MAX_GLOB_CACHE_ENTRY = 64;

// Timeout in milliseconds of cache entries for file handle cache.
inline static constexpr uint64_t DEFAULT_GLOB_CACHE_ENTRY_TIMEOUT_MILLISEC = 1800ULL * 1000 /*30min*/;

// Default option for profile type.
inline NoDestructor<string> DEFAULT_PROFILE_TYPE {*NOOP_PROFILE_TYPE};

// Default max number of parallel subrequest for a single filesystem read request. 0 means no limit.
inline uint64_t DEFAULT_MAX_SUBREQUEST_COUNT = 0;

// Default enable metadata cache.
inline bool DEFAULT_ENABLE_METADATA_CACHE = true;

// Default enable file handle cache.
inline bool DEFAULT_ENABLE_FILE_HANDLE_CACHE = true;

// Default enable glob cache.
inline bool DEFAULT_ENABLE_GLOB_CACHE = true;

// Default not ignore SIGPIPE in the extension.
inline bool DEFAULT_IGNORE_SIGPIPE = false;

// Default min disk bytes required for on-disk cache; by default 0 which user doesn't specify and override, and default
// value will be considered.
inline idx_t DEFAULT_MIN_DISK_BYTES_FOR_CACHE = 0;

//===--------------------------------------------------------------------===//
// Global configuration
//===--------------------------------------------------------------------===//

// Global configuration.
inline idx_t g_cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
inline bool g_ignore_sigpipe = DEFAULT_IGNORE_SIGPIPE;
inline NoDestructor<string> g_cache_type {*DEFAULT_CACHE_TYPE};
inline NoDestructor<string> g_profile_type {*DEFAULT_PROFILE_TYPE};
inline uint64_t g_max_subrequest_count = DEFAULT_MAX_SUBREQUEST_COUNT;

// On-disk cache configuration.
//
// Sorted cache directories.
inline NoDestructor<vector<string>> g_on_disk_cache_directories {*DEFAULT_ON_DISK_CACHE_DIRECTORY};
inline idx_t g_min_disk_bytes_for_cache = DEFAULT_MIN_DISK_BYTES_FOR_CACHE;
inline NoDestructor<string> g_on_disk_eviction_policy {*DEFAULT_ON_DISK_EVICTION_POLICY};

// In-memory cache configuration.
inline idx_t g_max_in_mem_cache_block_count = DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT;
inline idx_t g_in_mem_cache_block_timeout_millisec = DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC;

// Metadata cache configuration.
inline bool g_enable_metadata_cache = DEFAULT_ENABLE_METADATA_CACHE;
inline idx_t g_max_metadata_cache_entry = DEFAULT_MAX_METADATA_CACHE_ENTRY;
inline idx_t g_metadata_cache_entry_timeout_millisec = DEFAULT_METADATA_CACHE_ENTRY_TIMEOUT_MILLISEC;

// File handle cache configuration.
inline bool g_enable_file_handle_cache = DEFAULT_ENABLE_FILE_HANDLE_CACHE;
inline idx_t g_max_file_handle_cache_entry = DEFAULT_MAX_FILE_HANDLE_CACHE_ENTRY;
inline idx_t g_file_handle_cache_entry_timeout_millisec = DEFAULT_FILE_HANDLE_CACHE_ENTRY_TIMEOUT_MILLISEC;

// File glob configuration.
inline bool g_enable_glob_cache = DEFAULT_ENABLE_GLOB_CACHE;
inline idx_t g_max_glob_cache_entry = DEFAULT_MAX_GLOB_CACHE_ENTRY;
inline idx_t g_glob_cache_entry_timeout_millisec = DEFAULT_GLOB_CACHE_ENTRY_TIMEOUT_MILLISEC;

// Used for testing purpose, which has a higher priority over [g_cache_type], and won't be reset.
// TODO(hjiang): A better is bake configuration into `FileOpener`.
inline NoDestructor<string> g_test_cache_type {""};

// Used for testing purpose, which disable on-disk cache if true.
inline bool g_test_insufficient_disk_space = false;

//===--------------------------------------------------------------------===//
// Util function for filesystem configurations.
//===--------------------------------------------------------------------===//

// Get on-disk directories config.
std::vector<string> GetCacheDirectoryConfig(optional_ptr<FileOpener> opener);

// Set global cache filesystem configuration.
void SetGlobalConfig(optional_ptr<FileOpener> opener);

// Reset all global cache filesystem configuration.
void ResetGlobalConfig();

// Get concurrent IO sub-request count.
uint64_t GetThreadCountForSubrequests(uint64_t io_request_count);

} // namespace duckdb

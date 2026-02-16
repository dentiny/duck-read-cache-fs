#pragma once

#include <cstdint>
#include <optional>

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "no_destructor.hpp"
#include "size_literals.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Config constant
//===--------------------------------------------------------------------===//
extern const NoDestructor<string> NOOP_CACHE_TYPE;
extern const NoDestructor<string> ON_DISK_CACHE_TYPE;
extern const NoDestructor<string> IN_MEM_CACHE_TYPE;
extern const NoDestructor<unordered_set<string>> ALL_CACHE_TYPES;

// Cache reader names.
extern const NoDestructor<string> NOOP_CACHE_READER_NAME;
extern const NoDestructor<string> ON_DISK_CACHE_READER_NAME;
extern const NoDestructor<string> IN_MEM_CACHE_READER_NAME;

// Creation timestamp-based on-disk eviction policy.
extern const NoDestructor<string> ON_DISK_CREATION_TIMESTAMP_EVICTION;
// On-disk LRU eviction policy made for single-process usage.
extern const NoDestructor<string> ON_DISK_LRU_SINGLE_PROC_EVICTION;

// Default profile option, which performs no-op.
extern const NoDestructor<string> NOOP_PROFILE_TYPE;
// Store the latest IO operation profiling result, which potentially suffers concurrent updates.
extern const NoDestructor<string> TEMP_PROFILE_TYPE;
// Store the IO operation profiling results into duckdb table, which unblocks advanced analysis.
extern const NoDestructor<string> PERSISTENT_PROFILE_TYPE;
extern const NoDestructor<unordered_set<string>> ALL_PROFILE_TYPES;

//===--------------------------------------------------------------------===//
// Default configuration
//===--------------------------------------------------------------------===//
extern const idx_t DEFAULT_CACHE_BLOCK_SIZE;

// Default to use on-disk cache filesystem.
extern NoDestructor<string> DEFAULT_CACHE_TYPE;

// Default to timestamp-based on-disk cache eviction policy.
extern NoDestructor<string> DEFAULT_ON_DISK_EVICTION_POLICY;

// To prevent go out of disk space, we set a threshold to disallow local caching if insufficient. It applies to all
// filesystems. The value here is the decimal representation for percentage value; for example, 0.05 means 5%.
extern const double MIN_DISK_SPACE_PERCENTAGE_FOR_CACHE;

// By default, enable in-memory cache for disk cache reader.
extern const bool DEFAULT_ENABLE_DISK_READER_MEM_CACHE;

// Maximum in-memory cache block for disk cache reader.
extern const idx_t DEFAULT_MAX_DISK_READER_MEM_CACHE_BLOCK_COUNT;

// Default timeout in milliseconds for in-memory cache block for disk cache reader.
extern const idx_t DEFAULT_DISK_READER_MEM_CACHE_TIMEOUT_MILLISEC;

// Maximum in-memory cache block number, which caps the overall memory consumption as (block size * max block count).
extern const idx_t DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT;

// Default timeout in milliseconds for in-memory block cache entries.
extern const idx_t DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC;

// Max number of cache entries for file metadata cache.
extern const size_t DEFAULT_MAX_METADATA_CACHE_ENTRY;

// Timeout in milliseconds of cache entries for file metadata cache.
extern const uint64_t DEFAULT_METADATA_CACHE_ENTRY_TIMEOUT_MILLISEC;

// Number of seconds which we define as the threshold of staleness for metadata entries.
extern const idx_t CACHE_FILE_STALENESS_SECOND;
// Number of milliseconds which mark staleness.
extern const idx_t CACHE_FILE_STALENESS_MILLISEC;
// Number of microseconds which marks staleness.
extern const idx_t CACHE_FILE_STALENESS_MICROSEC;

// Max number of cache entries for file handle cache.
extern const size_t DEFAULT_MAX_FILE_HANDLE_CACHE_ENTRY;

// Timeout in milliseconds of cache entries for file handle cache.
extern const uint64_t DEFAULT_FILE_HANDLE_CACHE_ENTRY_TIMEOUT_MILLISEC;

// Max number of cache entries for glob cache.
extern const size_t DEFAULT_MAX_GLOB_CACHE_ENTRY;

// Timeout in milliseconds of cache entries for file handle cache.
extern const uint64_t DEFAULT_GLOB_CACHE_ENTRY_TIMEOUT_MILLISEC;

// Default option for profile type.
extern NoDestructor<string> DEFAULT_PROFILE_TYPE;

// Default max number of parallel subrequest for a single filesystem read request. 0 means no limit.
extern uint64_t DEFAULT_MAX_SUBREQUEST_COUNT;

// Default enable metadata cache.
extern bool DEFAULT_ENABLE_METADATA_CACHE;

// Default enable file handle cache.
extern bool DEFAULT_ENABLE_FILE_HANDLE_CACHE;

// Default enable glob cache.
extern bool DEFAULT_ENABLE_GLOB_CACHE;

// Default enable cache validation via version tag and last modification timestamp.
extern bool DEFAULT_ENABLE_CACHE_VALIDATION;

// Default enable clearing cache on write operations.
extern bool DEFAULT_CLEAR_CACHE_ON_WRITE;

// Default not ignore SIGPIPE in the extension.
extern bool DEFAULT_IGNORE_SIGPIPE;

// Default min disk bytes required for on-disk cache; by default 0 which user doesn't specify and override, and default
// value will be considered.
extern idx_t DEFAULT_MIN_DISK_BYTES_FOR_CACHE;

//===--------------------------------------------------------------------===//
// Util function for filesystem configurations.
//===--------------------------------------------------------------------===//

// Get on-disk directories config.
std::vector<string> GetCacheDirectoryConfig(optional_ptr<FileOpener> opener);

// Get concurrent IO sub-request count.
// If max_subrequest_count is 0, uses a default cap of 1024.
uint64_t GetThreadCountForSubrequests(uint64_t io_request_count, uint64_t max_subrequest_count);

} // namespace duckdb

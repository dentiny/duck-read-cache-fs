#include "cache_filesystem_config.hpp"

#include <cstdint>
#include <utility>

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "size_literals.hpp"
#include "utils/include/filesystem_utils.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Config constant definitions
//===--------------------------------------------------------------------===//
const NoDestructor<string> NOOP_CACHE_TYPE {"noop"};
const NoDestructor<string> ON_DISK_CACHE_TYPE {"on_disk"};
const NoDestructor<string> IN_MEM_CACHE_TYPE {"in_mem"};
const NoDestructor<unordered_set<string>> ALL_CACHE_TYPES {{*NOOP_CACHE_TYPE, *ON_DISK_CACHE_TYPE, *IN_MEM_CACHE_TYPE}};

// Cache reader names.
const NoDestructor<string> NOOP_CACHE_READER_NAME {"noop_cache_reader"};
const NoDestructor<string> ON_DISK_CACHE_READER_NAME {"on_disk_cache_reader"};
const NoDestructor<string> IN_MEM_CACHE_READER_NAME {"in_mem_cache_reader"};

// Creation timestamp-based on-disk eviction policy.
const NoDestructor<string> ON_DISK_CREATION_TIMESTAMP_EVICTION {"creation_timestamp"};
// On-disk LRU eviction policy made for single-process usage.
const NoDestructor<string> ON_DISK_LRU_SINGLE_PROC_EVICTION {"lru_sp"};

// Default profile option, which performs no-op.
const NoDestructor<string> NOOP_PROFILE_TYPE {"noop"};
// Store the latest IO operation profiling result, which potentially suffers concurrent updates.
const NoDestructor<string> TEMP_PROFILE_TYPE {"temp"};
// Store the IO operation profiling results into duckdb table, which unblocks advanced analysis.
const NoDestructor<string> PERSISTENT_PROFILE_TYPE {"duckdb"};
const NoDestructor<unordered_set<string>> ALL_PROFILE_TYPES {
    {*NOOP_PROFILE_TYPE, *TEMP_PROFILE_TYPE, *PERSISTENT_PROFILE_TYPE}};

// No clear cache on write.
const NoDestructor<string> DISABLE_CLEAR_CACHE_ON_WRITE {"disable"};
// Only clear cache on write for the current duckdb instance, which allows optimization to manage cache list in the
// memory, instead of listing filesystem every cleanup. The caveat is if multiple duckdb instances use the same cache
// directory and access the same file, there could be inconsistency.
const NoDestructor<string> CLEAR_CACHE_ON_WRITE_CUR_DB {"clear_for_cur_db"};
// The safest way to clear cache on write: list all cache files under the current cache directory and cleanup all
// related entries.
const NoDestructor<string> CLEAR_CACHE_ON_WRITE {"clear_cache_consistent"};
const NoDestructor<unordered_set<string>> ALL_CLEAR_CACHE_OPTIONS {
    {*DISABLE_CLEAR_CACHE_ON_WRITE, *CLEAR_CACHE_ON_WRITE_CUR_DB, *CLEAR_CACHE_ON_WRITE}};

//===--------------------------------------------------------------------===//
// Default configuration definitions
//===--------------------------------------------------------------------===//
const idx_t DEFAULT_CACHE_BLOCK_SIZE = 512_KiB;

// Default to use on-disk cache filesystem.
NoDestructor<string> DEFAULT_CACHE_TYPE {*ON_DISK_CACHE_TYPE};

// Default to timestamp-based on-disk cache eviction policy.
NoDestructor<string> DEFAULT_ON_DISK_EVICTION_POLICY {*ON_DISK_CREATION_TIMESTAMP_EVICTION};

// To prevent go out of disk space, we set a threshold to disallow local caching if insufficient. It applies to all
// filesystems. The value here is the decimal representation for percentage value; for example, 0.05 means 5%.
const double MIN_DISK_SPACE_PERCENTAGE_FOR_CACHE = 0.05;

// By default, enable in-memory cache for disk cache reader.
const bool DEFAULT_ENABLE_DISK_READER_MEM_CACHE = true;

// Maximum in-memory cache block for disk cache reader.
const idx_t DEFAULT_MAX_DISK_READER_MEM_CACHE_BLOCK_COUNT = 256;

// Default timeout in milliseconds for in-memory cache block for disk cache reader.
const idx_t DEFAULT_DISK_READER_MEM_CACHE_TIMEOUT_MILLISEC = 1800ULL * 1000 /*30min*/;

// Maximum in-memory cache block number, which caps the overall memory consumption as (block size * max block count).
const idx_t DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT = 256;

// Default timeout in milliseconds for in-memory block cache entries.
const idx_t DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC = 3600ULL * 1000 /*1hour*/;

// Max number of cache entries for file metadata cache.
const size_t DEFAULT_MAX_METADATA_CACHE_ENTRY = 250;

// Timeout in milliseconds of cache entries for file metadata cache.
const uint64_t DEFAULT_METADATA_CACHE_ENTRY_TIMEOUT_MILLISEC = 3600ULL * 1000 /*1hour*/;

// Number of seconds which we define as the threshold of staleness for metadata entries.
const idx_t CACHE_FILE_STALENESS_SECOND = 24 * 3600; // 1 day
// Number of milliseconds which mark staleness.
const idx_t CACHE_FILE_STALENESS_MILLISEC = CACHE_FILE_STALENESS_SECOND * 1000;
// Number of microseconds which marks staleness.
const idx_t CACHE_FILE_STALENESS_MICROSEC = CACHE_FILE_STALENESS_MILLISEC * 1000;

// Max number of cache entries for file handle cache.
const size_t DEFAULT_MAX_FILE_HANDLE_CACHE_ENTRY = 250;

// Timeout in milliseconds of cache entries for file handle cache.
const uint64_t DEFAULT_FILE_HANDLE_CACHE_ENTRY_TIMEOUT_MILLISEC = 3600ULL * 1000 /*1hour*/;

// Max number of cache entries for glob cache.
const size_t DEFAULT_MAX_GLOB_CACHE_ENTRY = 64;

// Timeout in milliseconds of cache entries for file handle cache.
const uint64_t DEFAULT_GLOB_CACHE_ENTRY_TIMEOUT_MILLISEC = 1800ULL * 1000 /*30min*/;

// Default option for profile type.
NoDestructor<string> DEFAULT_PROFILE_TYPE {*NOOP_PROFILE_TYPE};

// Default max number of parallel subrequest for a single filesystem read request. 0 means no limit.
uint64_t DEFAULT_MAX_SUBREQUEST_COUNT = 0;

// Default enable metadata cache.
bool DEFAULT_ENABLE_METADATA_CACHE = true;

// Default enable file handle cache.
bool DEFAULT_ENABLE_FILE_HANDLE_CACHE = true;

// Default enable glob cache.
bool DEFAULT_ENABLE_GLOB_CACHE = true;

// Default enable cache validation via version tag and last modification timestamp.
bool DEFAULT_ENABLE_CACHE_VALIDATION = false;

// Default disable clearing cache on write operations.
NoDestructor<string> DEFAULT_CLEAR_CACHE_ON_WRITE {*DISABLE_CLEAR_CACHE_ON_WRITE};

// Default not ignore SIGPIPE in the extension.
bool DEFAULT_IGNORE_SIGPIPE = false;

// Default min disk bytes required for on-disk cache; by default 0 which user doesn't specify and override, and default
// value will be considered.
idx_t DEFAULT_MIN_DISK_BYTES_FOR_CACHE = 0;

uint64_t GetThreadCountForSubrequests(uint64_t io_request_count, uint64_t max_subrequest_count) {
	if (max_subrequest_count == 0) {
		// Different platforms have different limits on the number of threads, use 1000 as the hard cap, above which
		// also increases context switch overhead.
		static constexpr uint64_t MAX_THREAD_COUNT = 1024;
		return MinValue<uint64_t>(io_request_count, MAX_THREAD_COUNT);
	}
	return MinValue<uint64_t>(io_request_count, max_subrequest_count);
}

} // namespace duckdb

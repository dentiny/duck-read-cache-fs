// Utils on filesystem operations.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Evict stale cache files.
//
// The function iterates all cache files under the directory, and performs a stat call on each of them, could be a
// performance bottleneck. But as the initial implementation, cache eviction only happens when insufficient disk space
// detected, which happens rarely thus not a big concern.
//
// Return the deleted cache files.
vector<string> EvictStaleCacheFiles(FileSystem &local_filesystem, const string &cache_directory);

// Get the number of files under the given local filesystem [folder].
int GetFileCountUnder(const string &folder);

// Get all files under the given local filesystem [folder] in alphabetically
// ascending order.
vector<string> GetSortedFilesUnder(const string &folder);

// Get all disk space in bytes for the filesystem indicated by the given [path].
idx_t GetOverallFileSystemDiskSpace(const string &path);

// Get total disk space in bytes for the filesystem at the given [path].
optional_idx GetTotalDiskSpace(const string &path);

// Get all on-disk cache files and sorted them in their creation timestamp.
map<timestamp_t, string> GetOnDiskFilesUnder(const vector<string> &folders);

// Update file access and modification timestamps to the current time.
// Return whether the update operation succeeds.
bool UpdateFileTimestamps(const string &filepath);

// Store the version string in the file's extended attributes
// Returns whether the operation succeeds.
bool SetCacheVersion(const string &filepath, const string &version);

// Retrieve the version string from the file's extended attributes.
// Returns empty string if missing or error.
string GetCacheVersion(const string &filepath);

// Check if caching is allowed (sufficient disk space)
bool CanCacheOnDisk(const string &cache_directory, idx_t cache_block_size, idx_t min_disk_bytes_for_cache);

// Get the system temporary directory path for the current platform.
string GetTemporaryDirectory();

// Get the default on-disk cache directory path.
// Returns a path in the system temporary directory with the cache subdirectory name.
const string &GetDefaultOnDiskCacheDirectory();

// Get the fake filesystem directory path.
const string &GetFakeOnDiskCacheDirectory();

// Get filesystem page size.
idx_t GetFileSystemPageSize();

struct MaxFileNameLength {
	idx_t max_filepath_len = 0;
	idx_t max_filename_len = 0;
};

// Get the platform maximum for full file path length and single filename component length.
MaxFileNameLength GetMaxFileNameLength();

// Get the maximum size in bytes for a single extended attribute value on the current platform.
// Linux: XATTR_SIZE_MAX from <linux/xattr.h> (64 KiB on ext2/3/4).
// macOS: XATTR_MAXSIZE from <sys/xattr.h> (INT32_MAX on APFS/HFS+).
// Windows: No platform constant; ADS have no fixed small-size limit, 64 KiB used as practical chunk size.
idx_t GetMaxXattrValueSize();

} // namespace duckdb

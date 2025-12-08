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

// Return whether we could cache content in the filesystem specified by the given [path].
bool CanCacheOnDisk(const string &path);

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

} // namespace duckdb

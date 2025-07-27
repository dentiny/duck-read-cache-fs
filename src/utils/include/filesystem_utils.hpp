// Utils on filesystem operations.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

// Evict stale cache files.
//
// The function iterates all cache files under the directory, and performs a
// stat call on each of them, could be a performance bottleneck. But as the
// initial implementation, cache eviction only happens when insufficient disk
// space detected, which happens rarely thus not a big concern.
void EvictStaleCacheFiles(FileSystem &local_filesystem, const string &cache_directory);

// Get the number of files under the given local filesystem [folder].
int GetFileCountUnder(const string &folder);

// Get all files under the given local filesystem [folder] in alphabetically
// ascending order.
vector<string> GetSortedFilesUnder(const string &folder);

// Get all disk space in bytes for the filesystem indicated by the given [path].
idx_t GetOverallFileSystemDiskSpace(const string &path);

// Return whether we could cache content in the filesystem specified by the given [path].
bool CanCacheOnDisk(const string &path);

} // namespace duckdb

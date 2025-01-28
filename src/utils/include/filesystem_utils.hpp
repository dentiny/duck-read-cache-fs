// Utils on filesystem operations.

#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

// Evict stale cache files.
//
// The function iterates all cache files under the directory, and performs a
// stat call on each of them, could be a performance bottleneck. But as the
// initial implementation, cache eviction only happens when insufficient disk
// space detected, which happens rarely thus not a big concern.
void EvictStaleCacheFiles(FileSystem &local_filesystem,
                          const string &cache_directory);

} // namespace duckdb

// Tracks the total size in bytes for finalized cache files under the on-disk cache directories, so the cache's own
// footprint could be bounded with `cache_httpfs_max_on_disk_cache_size`.
//
// The tracked value is a per-process approximation:
// - It's lazily initialized by scanning all cache directories on the first use;
// - It's incrementally updated when cache files are stored and evicted afterwards;
// - It's invalidated (and lazily rebuilt) by operations which change cache files in bulk.
// Concurrent cache writers and other processes sharing the same cache directories could make the tracked value
// temporarily off by a few blocks, which is acceptable and self-correcting, similar to the best-effort disk space
// check performed before cache file writes.

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "mutex.hpp"
#include "optional.hpp"
#include "thread_annotation.hpp"

namespace duckdb {

class FileSystem;

class DiskCacheFootprintTracker {
public:
	DiskCacheFootprintTracker() = default;
	DiskCacheFootprintTracker(const DiskCacheFootprintTracker &) = delete;
	DiskCacheFootprintTracker &operator=(const DiskCacheFootprintTracker &) = delete;

	// Return the tracked total cache file size in bytes; scans [cache_directories] first if not loaded.
	// In-flight temporary cache files are excluded from the scan, since they're transient and deleted after cache file
	// finalization.
	idx_t GetOrLoad(const vector<string> &cache_directories);

	// Increase the tracked total cache file size by [bytes]; no-op if not loaded.
	void Add(idx_t bytes);

	// Decrease the tracked total cache file size by [bytes], floored at zero; no-op if not loaded.
	void Subtract(idx_t bytes);

	// Decrease the tracked total cache file size by the current size for the file at [filepath]; no-op if not loaded
	// or the file doesn't exist. Should be called before the file gets deleted.
	void SubtractFileSize(FileSystem &local_filesystem, const string &filepath);

	// Whether the tracked total cache file size has been loaded.
	bool IsLoaded() const;

	// Drop the tracked total cache file size, so the next [GetOrLoad] re-scans cache directories.
	void Invalidate();

private:
	mutable concurrency::mutex mutex;
	// Total size in bytes for all finalized cache files; [nullopt] means not loaded yet.
	optional<idx_t> total_cache_file_bytes DUCKDB_GUARDED_BY(mutex);
};

} // namespace duckdb

// Eviction policy for on-disk cache files, which decides cache files to delete when disk space runs low.

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Forward declarations.
struct InstanceConfig;

// All methods are thread-safe.
class DiskCacheEvictionPolicy {
public:
	virtual ~DiskCacheEvictionPolicy() = default;

	// Policy name, which matches the value of `cache_httpfs_evict_policy`.
	virtual const string &GetName() const = 0;

	// Called after a cache hit on [cache_filepath].
	virtual void OnCacheFileAccess(const string &cache_filepath) = 0;

	// Called after a new cache file is persisted at [cache_filepath].
	virtual void OnCacheFileStore(const string &cache_filepath) = 0;

	// Called after [cache_filepaths] are deleted outside of eviction, i.e. cache invalidation for a remote file.
	virtual void OnCacheFilesRemoved(const vector<string> &cache_filepaths) = 0;

	// Called after all cache files are deleted.
	virtual void OnCacheCleared() = 0;

	// Delete cache files to release disk space, called when [cache_directory] doesn't have enough space for new cache
	// files.
	virtual void Evict(const string &cache_directory, const InstanceConfig &config) = 0;
};

// Create the on-disk eviction policy named [policy_name].
unique_ptr<DiskCacheEvictionPolicy> CreateDiskCacheEvictionPolicy(const string &policy_name);

} // namespace duckdb

// CacheFsRefRegistry is a singleton registry which stores references for all cache filesystems.
// The class is not thread-safe.

#pragma once

#include "duckdb/common/vector.hpp"

namespace duckdb {

// Forward declaration.
class CacheFileSystem;

class CacheFsRefRegistry {
public:
	// Get the singleton instance for the registry.
	static CacheFsRefRegistry &Get();

	// Register the cache filesystem to the registry.
	void Register(CacheFileSystem *fs);

	// Reset the registry.
	void Reset();

	// Get all cache filesystems.
	const vector<CacheFileSystem *> &GetAllCacheFs() const;

private:
	CacheFsRefRegistry() = default;

	// The ownership lies in db instance.
	vector<CacheFileSystem *> cache_filesystems;
};

} // namespace duckdb

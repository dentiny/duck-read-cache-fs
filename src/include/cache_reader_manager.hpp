// A class which manages all cache readers and is shared among all cache filesystems.
// It's designed as singleton instead of shared pointer passed to everywhere, because it's accessed in quite a few
// places, and it's not easy to pass around shared pointer in all cases.

#pragma once

#include "base_cache_reader.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class CacheReaderManager {
public:
	static CacheReaderManager &Get();

	// Set cache reader if uninitialized.
	void SetCacheReader();

	// Get current cache reader.
	BaseCacheReader *GetCacheReader() const;

	// Get all cache readers if they're initialized.
	vector<BaseCacheReader *> GetCacheReaders() const;

	// Clear cache for all cache readers.
	void ClearCache();

	// Clear cache for all cache readers on the given [fname].
	void ClearCache(const string &fname);

	// Reset all cache readers.
	void Reset();

private:
	CacheReaderManager() = default;

	// Noop, in-memory and on-disk cache reader.
	unique_ptr<BaseCacheReader> noop_cache_reader;
	unique_ptr<BaseCacheReader> in_mem_cache_reader;
	unique_ptr<BaseCacheReader> on_disk_cache_reader;
	// Either in-memory or on-disk cache reader, whichever is actively being used, ownership lies the above cache
	// reader.
	BaseCacheReader *internal_cache_reader = nullptr;
};

} // namespace duckdb

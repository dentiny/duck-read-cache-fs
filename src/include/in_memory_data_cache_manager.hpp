// Interface class for in-memory data cache management.

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Abstract interface for managing in-memory data cache.
// Different implementations can use different backends (e.g., custom LRU cache, DuckDB's BufferPool).
template <typename Key, typename Val, typename KeyCompare = std::less<Key>>
class InMemoryDataCacheManager {
public:
	virtual ~InMemoryDataCacheManager() = default;

	// Insert value with the given key.
	// This will replace any previous entry with the same key.
	virtual void Put(Key key, shared_ptr<Val> value) = 0;

	// Look up the entry with the given key.
	// Returns nullptr if key doesn't exist in cache or if entry is expired.
	virtual shared_ptr<Val> Get(const Key &key) = 0;

	// Delete the entry with the given key.
	// Returns true if the entry was found and deleted, false otherwise.
	virtual bool Delete(const Key &key) = 0;

	// Clear all cache entries.
	virtual void Clear() = 0;

	// Clear cache entries that match the given filter starting from start_key inclusively.
	// Stops at the first non-matched entry.
	// @param start_key: The key to start clearing from (inclusive)
	// @param key_filter: A function that returns true for keys that should be cleared
	template <typename KeyFilter>
	void Clear(const Key &start_key, KeyFilter &&key_filter) {
		ClearWithFilter(start_key, std::forward<KeyFilter>(key_filter));
	}

	// Get all keys currently in the cache.
	// The order of keys returned is not guaranteed to be deterministic.
	virtual vector<Key> Keys() const = 0;

	// Get the maximum number of entries the cache can hold.
	// Returns 0 if there is no explicit entry limit (e.g., managed by memory budget).
	virtual size_t MaxEntries() const = 0;

protected:
	// Type-erased version of Clear with filter for virtual dispatch.
	// Implementations should override this method.
	virtual void ClearWithFilter(const Key &start_key, std::function<bool(const Key &)> key_filter) = 0;
};

} // namespace duckdb

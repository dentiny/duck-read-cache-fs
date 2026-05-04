// Pluggable storage backend for the in-memory data block cache.

#pragma once

#include <functional>
#include <utility>

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "in_mem_cache_block.hpp"
#include "in_mem_cache_data_entry.hpp"

namespace duckdb {

class InMemoryDataCacheStorage {
public:
	virtual ~InMemoryDataCacheStorage() = default;

	// Insert or replace the entry for [key].
	virtual void Put(InMemCacheBlock key, shared_ptr<InMemCacheDataEntry> value) = 0;

	// Returns nullptr on miss or if the entry is no longer valid (e.g. timed out).
	virtual shared_ptr<InMemCacheDataEntry> Get(const InMemCacheBlock &key) = 0;

	// Returns true if [key] was present and removed.
	virtual bool Delete(const InMemCacheBlock &key) = 0;

	virtual void Clear() = 0;

	// Bulk delete for keys >= [start_key] for which [filter] returns true; stops at the first non-matching key.
	// Used for prefix-bounded invalidation (e.g. clear all blocks for one file).
	virtual void Clear(const InMemCacheBlock &start_key, std::function<bool(const InMemCacheBlock &)> filter) = 0;

	// Snapshot of currently-live keys; ordering is unspecified.
	virtual vector<InMemCacheBlock> Keys() const = 0;

	// Drain all entries.
	virtual vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> Take() = 0;
};

} // namespace duckdb

// Pluggable storage backend for the in-memory data block cache.

#pragma once

#include <functional>
#include <utility>

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "in_mem_cache_block.hpp"
#include "in_mem_cache_data_entry.hpp"
#include "optional.hpp"
#include "page_aligned_data_chunk.hpp"

#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

// Forward declarations.
class DatabaseInstance;

class PinnedBlock {
public:
	PinnedBlock(shared_ptr<void> keep_alive_p, const PageAlignedDataChunk *chunk_p)
	    : keep_alive(std::move(keep_alive_p)), chunk(chunk_p) {
	}

	const PageAlignedDataChunk &Data() const {
		return *chunk;
	}

	static bool ValidateVersionTag(const string &cached, const string &expected_version_tag) {
		return expected_version_tag.empty() || cached == expected_version_tag;
	}

private:
	shared_ptr<void> keep_alive;
	// Owned by `keep_alive`.
	const PageAlignedDataChunk *chunk;
};

class InMemoryDataCacheStorage {
public:
	virtual ~InMemoryDataCacheStorage() = default;

	// Insert or replace the entry for [key]. Storage takes ownership of [chunk].
	virtual void Put(InMemCacheBlock key, PageAlignedDataChunk chunk, string version_tag) = 0;

	// Returns nullopt on miss or if the entry is no longer valid (e.g. timed out, version mismatch).
	virtual optional<PinnedBlock> Get(const InMemCacheBlock &key, const string &expected_version_tag) = 0;

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

// Construct the storage backend selected by [mode].
shared_ptr<InMemoryDataCacheStorage> BuildInMemoryDataCacheStorage(const string &mode,
                                                                   optional_ptr<DatabaseInstance> db_instance,
                                                                   size_t max_entries, uint64_t timeout_millisec);

} // namespace duckdb

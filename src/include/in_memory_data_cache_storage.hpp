// Pluggable storage backend for the in-memory data block cache.

#pragma once

#include <functional>
#include <utility>

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "in_mem_cache_block.hpp"
#include "in_mem_cache_data_entry.hpp"
#include "page_aligned_data_chunk.hpp"

namespace duckdb {

// RAII view over a cached block returned by `InMemoryDataCacheStorage::Get`.
// The block stays alive (and the underlying bytes valid) for the lifetime of the PinnedBlock.
//
// In `extension` mode `keep_alive` owns a `shared_ptr<InMemCacheDataEntry>` (the LRU map's value).
// In `object_cache` mode it owns a `shared_ptr<CacheHttpfsDataBlock>` fetched from ObjectCache,
// which pins the block against concurrent eviction for the duration of the read.
class PinnedBlock {
public:
	PinnedBlock(shared_ptr<void> keep_alive, const PageAlignedDataChunk *chunk, string version_tag)
	    : keep_alive_(std::move(keep_alive)), chunk_(chunk), version_tag_(std::move(version_tag)) {
	}

	const PageAlignedDataChunk &Data() const {
		return *chunk_;
	}
	const string &VersionTag() const {
		return version_tag_;
	}

private:
	shared_ptr<void> keep_alive_;
	const PageAlignedDataChunk *chunk_;
	string version_tag_;
};

class InMemoryDataCacheStorage {
public:
	virtual ~InMemoryDataCacheStorage() = default;

	// Insert or replace the entry for [key]. Storage takes ownership of [chunk].
	virtual void Put(InMemCacheBlock key, PageAlignedDataChunk chunk, string version_tag) = 0;

	// Returns nullptr on miss or if the entry is no longer valid (e.g. timed out).
	// The returned PinnedBlock pins the underlying bytes for the duration of its lifetime.
	virtual unique_ptr<PinnedBlock> Get(const InMemCacheBlock &key) = 0;

	// Returns true if [key] was present and removed.
	virtual bool Delete(const InMemCacheBlock &key) = 0;

	virtual void Clear() = 0;

	// Bulk delete for keys >= [start_key] for which [filter] returns true; stops at the first non-matching key.
	// Used for prefix-bounded invalidation (e.g. clear all blocks for one file).
	virtual void Clear(const InMemCacheBlock &start_key, std::function<bool(const InMemCacheBlock &)> filter) = 0;

	// Snapshot of currently-live keys; ordering is unspecified.
	virtual vector<InMemCacheBlock> Keys() const = 0;

	// Drain all entries. Each returned entry owns its chunk (in `object_cache` mode the corresponding
	// ObjectCache entry has been removed before the chunk was moved out).
	virtual vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> Take() = 0;
};

} // namespace duckdb

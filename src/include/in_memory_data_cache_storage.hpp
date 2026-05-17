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

namespace duckdb {

class PinnedBlock {
public:
	PinnedBlock(shared_ptr<void> keep_alive_p, const PageAlignedDataChunk *chunk_p, string version_tag_p)
	    : keep_alive(std::move(keep_alive_p)), chunk(chunk_p), version_tag(std::move(version_tag_p)) {
	}

	const PageAlignedDataChunk &Data() const {
		return *chunk;
	}
	const string &VersionTag() const {
		return version_tag;
	}

private:
	shared_ptr<void> keep_alive;
	const PageAlignedDataChunk *chunk;
	string version_tag;
};

class InMemoryDataCacheStorage {
public:
	virtual ~InMemoryDataCacheStorage() = default;

	// Insert or replace the entry for [key]. Storage takes ownership of [chunk].
	virtual void Put(InMemCacheBlock key, PageAlignedDataChunk chunk, string version_tag) = 0;

	// Returns nullopt on miss or if the entry is no longer valid (e.g. timed out).
	// The returned PinnedBlock pins the underlying bytes for the duration of its lifetime.
	virtual optional<PinnedBlock> Get(const InMemCacheBlock &key) = 0;

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

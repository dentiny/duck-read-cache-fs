// Extension-managed in-memory data cache: bounded by entry count and per-entry timeout, with pluggable eviction policy.

#pragma once

#include <cstddef>
#include <cstdint>

#include "duckdb/common/map.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "eviction_policy.hpp"
#include "in_memory_data_cache_storage.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

namespace duckdb {

class ExtensionBoundedDataCacheStorage final : public InMemoryDataCacheStorage {
public:
	// @param max_entries: 0 means unbounded.
	// @param timeout_millisec: 0 means no timeout.
	// @param eviction_policy: decides which entry to evict when exceeding [max_entries].
	ExtensionBoundedDataCacheStorage(size_t max_entries, uint64_t timeout_millisec,
	                                 unique_ptr<EvictionPolicy<InMemCacheBlock>> eviction_policy);

	ExtensionBoundedDataCacheStorage(const ExtensionBoundedDataCacheStorage &) = delete;
	ExtensionBoundedDataCacheStorage &operator=(const ExtensionBoundedDataCacheStorage &) = delete;

	~ExtensionBoundedDataCacheStorage() override = default;

	void Put(InMemCacheBlock key, PageAlignedDataChunk chunk, string version_tag) override;
	optional<PinnedBlock> Get(const InMemCacheBlock &key, const string &expected_version_tag) override;
	bool Delete(const InMemCacheBlock &key) override;
	void Clear() override;
	void Clear(const InMemCacheBlock &start_key, std::function<bool(const InMemCacheBlock &)> filter) override;
	vector<InMemCacheBlock> Keys() const override;
	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> Take() override;

private:
	struct Entry {
		shared_ptr<InMemCacheDataEntry> value;
		// Steady clock timestamp when the entry was inserted or last replaced; not updated on reads.
		uint64_t timestamp = 0;
	};
	using EntryMap = map<InMemCacheBlock, Entry, InMemCacheBlockLess>;

	bool IsExpired(const Entry &entry, uint64_t now) const;
	// Remove the entry pointed by [iter] from both the cache and the eviction policy.
	void DeleteImpl(EntryMap::iterator iter) DUCKDB_REQUIRES(mu);

	const size_t max_entries;
	const uint64_t timeout_millisec;

	mutable concurrency::mutex mu;
	// Use ordered map for range deletion.
	EntryMap entries DUCKDB_GUARDED_BY(mu);
	unique_ptr<EvictionPolicy<InMemCacheBlock>> eviction_policy DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb

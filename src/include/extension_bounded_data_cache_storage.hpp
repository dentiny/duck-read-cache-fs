// Extension-managed in-memory data cache: bounded by entry count and per-entry timeout.

#pragma once

#include <cstddef>
#include <cstdint>

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "in_memory_data_cache_storage.hpp"
#include "shared_value_lru_cache.hpp"

namespace duckdb {

class ExtensionBoundedDataCacheStorage final : public InMemoryDataCacheStorage {
public:
	// @param max_entries: 0 means unbounded.
	// @param timeout_millisec: 0 means no timeout.
	ExtensionBoundedDataCacheStorage(size_t max_entries, uint64_t timeout_millisec);

	ExtensionBoundedDataCacheStorage(const ExtensionBoundedDataCacheStorage &) = delete;
	ExtensionBoundedDataCacheStorage &operator=(const ExtensionBoundedDataCacheStorage &) = delete;

	~ExtensionBoundedDataCacheStorage() override = default;

	void Put(InMemCacheBlock key, shared_ptr<InMemCacheDataEntry> value) override;
	shared_ptr<InMemCacheDataEntry> Get(const InMemCacheBlock &key) override;
	bool Delete(const InMemCacheBlock &key) override;
	void Clear() override;
	void Clear(const InMemCacheBlock &start_key, std::function<bool(const InMemCacheBlock &)> filter) override;
	vector<InMemCacheBlock> Keys() const override;
	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> Take() override;

private:
	ThreadSafeSharedValueLruCache<InMemCacheBlock, InMemCacheDataEntry, InMemCacheBlockLess> lru_cache;
};

} // namespace duckdb

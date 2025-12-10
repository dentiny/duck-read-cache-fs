// A filesystem wrapper, which performs in-memory cache for read operations.

#pragma once

#include "base_cache_reader.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "in_mem_cache_block.hpp"
#include "shared_lru_cache.hpp"

namespace duckdb {

// Forward declaration.
struct CacheHttpfsInstanceState;

class InMemoryCacheReader final : public BaseCacheReader {
public:
	// Constructor: config values are read from instance state at runtime (with defaults as fallback).
	explicit InMemoryCacheReader(weak_ptr<CacheHttpfsInstanceState> instance_state_p)
	    : instance_state(std::move(instance_state_p)) {
	}
	~InMemoryCacheReader() override = default;

	std::string GetName() const override {
		return "in_mem_cache_reader";
	}

	void ClearCache() override;
	void ClearCache(const string &fname) override;
	void ReadAndCache(FileHandle &handle, char *buffer, uint64_t requested_start_offset,
	                  uint64_t requested_bytes_to_read, uint64_t file_size,
	                  BaseProfileCollector *profile_collector = nullptr) override;
	vector<DataCacheEntryInfo> GetCacheEntriesInfo() const override;

private:
	// Cache entry wrapper that stores data along with validation metadata.
	struct InMemCacheEntry {
		string data;
		string version_tag;
	};

	using InMemCache =
	    ThreadSafeSharedLruCache<InMemCacheBlock, InMemCacheEntry, InMemCacheBlockHash, InMemCacheBlockEqual>;

	// Return whether the given cache entry is still valid and usable.
	bool ValidateCacheEntry(InMemCacheEntry *cache_entry, const string &version_tag);

	// Instance state for config lookup.
	weak_ptr<CacheHttpfsInstanceState> instance_state;

	// Once flag to guard against cache's initialization.
	std::once_flag cache_init_flag;
	// LRU cache to store blocks; late initialized after first access.
	unique_ptr<InMemCache> cache;
};

} // namespace duckdb

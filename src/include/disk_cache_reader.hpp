// A filesystem wrapper, which performs on-disk cache for read operations.

#pragma once

#include "base_cache_reader.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_read_chunk.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "in_mem_cache_block.hpp"
#include "shared_value_lru_cache.hpp"
#include "thread_annotation.hpp"

namespace duckdb {

// Forward declarations.
struct CacheHttpfsInstanceState;
struct InstanceConfig;

class DiskCacheReader final : public BaseCacheReader {
public:
	// Constructor: cache_directories defines where cache files are stored.
	DiskCacheReader(weak_ptr<CacheHttpfsInstanceState> instance_state_p, BaseProfileCollector &profile_collector_p);
	~DiskCacheReader() override = default;

	string GetName() const override {
		return *ON_DISK_CACHE_READER_NAME;
	}

	void ClearCache() override;
	void ClearCache(const string &fname) override;

	void ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset, idx_t requested_bytes_to_read,
	                  idx_t file_size) override;

	vector<DataCacheEntryInfo> GetCacheEntriesInfo() const override;

	// Get file cache block to evict.
	// Notice returned filepath will be removed from LRU list, but the actual file won't be deleted.
	string EvictCacheBlockLru();

private:
	struct InMemCacheEntry {
		string data;
		string version_tag;
	};

	using InMemCache = ThreadSafeSharedValueLruCache<InMemCacheBlock, InMemCacheEntry, InMemCacheBlockLess>;

	// Return whether the given cache entry is still valid and usable.
	bool ValidateCacheEntry(InMemCacheEntry *cache_entry, const string &version_tag);

	// Process a single cache read chunk in a worker thread.
	void ProcessCacheReadChunk(FileHandle &handle, const InstanceConfig &config, const string &version_tag,
	                           CacheReadChunk cache_read_chunk);

	// Used to access local cache files.
	unique_ptr<FileSystem> local_filesystem;
	// Used for on-disk cache block LRU-based eviction.
	concurrency::mutex cache_file_creation_timestamp_map_mutex;
	// Maps from last access timestamp to filepath.
	map<timestamp_t, string>
	    cache_file_creation_timestamp_map DUCKDB_GUARDED_BY(cache_file_creation_timestamp_map_mutex);
	// Once flag to guard against cache's initialization.
	std::once_flag cache_init_flag;
	// LRU cache to store blocks; late initialized after first access.
	// Used to avoid local disk IO.
	// NOTICE: cache key uses remote filepath, instead of local cache filepath.
	unique_ptr<InMemCache> in_mem_cache_blocks;
	// Instance state for config lookup.
	weak_ptr<CacheHttpfsInstanceState> instance_state;
};

} // namespace duckdb

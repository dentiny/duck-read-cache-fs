// A filesystem wrapper, which performs on-disk cache for read operations.

#pragma once

#include <mutex>

#include "base_cache_reader.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "in_mem_cache_block.hpp"
#include "shared_lru_cache.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;

class DiskCacheReader final : public BaseCacheReader {
public:
	explicit DiskCacheReader(optional_ptr<DatabaseInstance> duckdb_instance_p = nullptr);
	~DiskCacheReader() override = default;

	std::string GetName() const override {
		return "on_disk_cache_reader";
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
	using InMemCache = ThreadSafeSharedLruCache<InMemCacheBlock, string, InMemCacheBlockHash, InMemCacheBlockEqual>;

	// Attempt to cache [chunk] to local filesystem, if there's sufficient disk space available.
	// Otherwise, nothing happens.
	void CacheLocal(const FileHandle &handle, const string &cache_directory, const string &local_cache_file,
	                const string &content);

	// Used to access local cache files.
	unique_ptr<FileSystem> local_filesystem;
	// Used for on-disk cache block LRU-based eviction.
	std::mutex cache_file_creation_timestamp_map_mutex;
	// Maps from last access timestamp to filepath.
	map<timestamp_t, string> cache_file_creation_timestamp_map;
	// Once flag to guard against cache's initialization.
	std::once_flag cache_init_flag;
	// LRU cache to store blocks; late initialized after first access.
	// Used to avoid local disk IO.
	// NOTICE: cache key uses remote filepath, instead of local cache filepath.
	unique_ptr<InMemCache> in_mem_cache_blocks;
	// Duckdb instance, used for logging purpose.
	optional_ptr<DatabaseInstance> duckdb_instance;
};

} // namespace duckdb

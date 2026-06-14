// A filesystem wrapper, which performs on-disk cache for read operations.

#pragma once

#include "base_cache_reader.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_read_chunk.hpp"
#include "disk_cache_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "in_mem_cache_block.hpp"
#include "in_mem_cache_data_entry.hpp"
#include "in_memory_data_cache_storage.hpp"
#include "mutex.hpp"
#include "optional.hpp"
#include "thread_annotation.hpp"

namespace duckdb {

// Forward declarations.
struct CacheHttpfsInstanceState;
struct InstanceConfig;

class DiskCacheReader final : public BaseCacheReader {
public:
	explicit DiskCacheReader(weak_ptr<CacheHttpfsInstanceState> instance_state_p);
	~DiskCacheReader() override = default;

	string GetName() const override {
		return *ON_DISK_CACHE_READER_NAME;
	}

	void ClearCache() override;
	void ClearCache(const string &fname) override;

	void ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset, idx_t requested_bytes_to_read,
	                  idx_t file_size) override;

	vector<DataCacheEntryInfo> GetCacheEntriesInfo() const override;

	void RemapInMemoryDataBlocksForNewBlockSize(idx_t new_block_size) override;

	// Get file cache block to evict.
	// Notice returned filepath will be removed from LRU list, but the actual file won't be deleted.
	optional<string> EvictCacheBlockLru();

private:
	// Process a single cache read chunk in a worker thread.
	void ProcessCacheReadChunk(FileHandle &handle, const InstanceConfig &config, const string &version_tag,
	                           const DiskCacheUtil::RemoteFileCachePathInfo &path_info,
	                           CacheReadChunk cache_read_chunk);

	// Insert or refresh [filepath] in the LRU access maps and on-disk access time.
	void UpsertCacheFileAccessTimestamp(const string &filepath);

	// Remove [filepath] from the LRU access maps.
	void RemoveCacheFileAccessTimestamp(const string &filepath) DUCKDB_REQUIRES(cache_file_access_timestamp_map_mutex);

	// Rebuild LRU access maps from on-disk cache files.
	void LoadCacheFileAccessTimestampMapsFromDisk() DUCKDB_REQUIRES(cache_file_access_timestamp_map_mutex);

	// Used to access local cache files.
	unique_ptr<FileSystem> local_filesystem;
	// Used for on-disk cache block LRU-based eviction.
	concurrency::mutex cache_file_access_timestamp_map_mutex;
	// Maps last-access timestamp to cache filepath.
	map<timestamp_t, string> cache_file_access_timestamp_map DUCKDB_GUARDED_BY(cache_file_access_timestamp_map_mutex);
	// Maps from filepath to last-access timestamp.
	// Invariant: each filepath appears exactly once in both maps.
	unordered_map<string, timestamp_t>
	    cache_filepath_to_access_timestamp DUCKDB_GUARDED_BY(cache_file_access_timestamp_map_mutex);
	// Once flag to guard against cache's initialization.
	std::once_flag cache_init_flag;
	// In-memory cache to store blocks; late initialized after first access.
	// Used to avoid local disk IO.
	// NOTICE: cache key uses remote filepath, instead of local cache filepath.
	shared_ptr<InMemoryDataCacheStorage> in_mem_storage;
};

} // namespace duckdb

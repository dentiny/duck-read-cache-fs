// A filesystem wrapper, which performs on-disk cache for read operations.

#pragma once

#include "base_cache_reader.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_read_chunk.hpp"
#include "disk_cache_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "eviction_policy.hpp"
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

private:
	// Process a single cache read chunk in a worker thread.
	void ProcessCacheReadChunk(FileHandle &handle, const InstanceConfig &config, const string &version_tag,
	                           const DiskCacheUtil::RemoteFileCachePathInfo &path_info,
	                           CacheReadChunk cache_read_chunk);

	// Record an access to [cache_filepath], either a cache hit or a new cache file, for eviction.
	void RecordCacheFileAccess(const InstanceConfig &config, const string &cache_filepath);

	// Delete cache files to release disk space for [cache_directory].
	void EvictCacheFiles(const InstanceConfig &config, const string &cache_directory);

	// Get the eviction policy for [config], which is reset when the configured policy changes.
	// Return nullptr for creation timestamp based eviction, which doesn't depend on access history.
	EvictionPolicy<string> *GetEvictionPolicy(const InstanceConfig &config) DUCKDB_REQUIRES(eviction_policy_mutex);

	// Used to access local cache files.
	unique_ptr<FileSystem> local_filesystem;
	concurrency::mutex eviction_policy_mutex;
	// Name of the eviction policy [eviction_policy] is created for.
	string eviction_policy_name DUCKDB_GUARDED_BY(eviction_policy_mutex);
	// Keyed by local cache filepath; access history is only tracked within the current process.
	unique_ptr<EvictionPolicy<string>> eviction_policy DUCKDB_GUARDED_BY(eviction_policy_mutex);
	// Once flag to guard against cache's initialization.
	std::once_flag cache_init_flag;
	// In-memory cache to store blocks; late initialized after first access.
	// Used to avoid local disk IO.
	// NOTICE: cache key uses remote filepath, instead of local cache filepath.
	shared_ptr<InMemoryDataCacheStorage> in_mem_storage;
};

} // namespace duckdb

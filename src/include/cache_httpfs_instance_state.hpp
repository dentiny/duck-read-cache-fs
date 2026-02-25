// Per-instance state for cache_httpfs extension.
// State is stored in DuckDB's ObjectCache for automatic cleanup when DatabaseInstance is destroyed.

#pragma once

#include "base_cache_reader.hpp"
#include "cache_exclusion_manager.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "filesystem_utils.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

namespace duckdb {

// Forward declarations
class BaseProfileCollector;
class CacheFileSystem;
class ClientContext;
class DatabaseInstance;
class FileOpener;
struct CacheHttpfsInstanceState;

//===--------------------------------------------------------------------===//
// Per-instance filesystem registry
//===--------------------------------------------------------------------===//
class InstanceCacheFsRegistry {
public:
	// Register `fs` in the registry. Registering the same CacheFileSystem for multiple time is harmless but useless.
	void Register(CacheFileSystem *fs);
	// Unregister `fs` from the registry. Unregistering a CacheFileSystem that is not registered doesn't have any
	// effect.
	void Unregister(CacheFileSystem *fs);
	unordered_set<CacheFileSystem *> GetAllCacheFs() const;
	void Reset();

private:
	mutable concurrency::mutex mutex;
	unordered_set<CacheFileSystem *> cache_filesystems DUCKDB_GUARDED_BY(mutex);
};

//===--------------------------------------------------------------------===//
// Per-instance cache reader manager
//===--------------------------------------------------------------------===//

// Forward declaration
struct InstanceConfig;

class InstanceCacheReaderManager {
public:
	void SetCacheReader(const InstanceConfig &config, weak_ptr<CacheHttpfsInstanceState> instance_state_p);
	BaseCacheReader *GetCacheReader() const;
	vector<BaseCacheReader *> GetCacheReaders() const;
	void InitializeDiskCacheReader(const vector<string> &cache_directories,
	                               weak_ptr<CacheHttpfsInstanceState> instance_state_p);
	// Update existing readers to use a new profile collector (when profile type changes).
	void UpdateProfileCollector(BaseProfileCollector &profile_collector);
	void ClearCache();
	void ClearCache(const string &fname);
	void Reset();

private:
	mutable concurrency::mutex mutex;
	unique_ptr<BaseCacheReader> noop_cache_reader DUCKDB_GUARDED_BY(mutex);
	unique_ptr<BaseCacheReader> in_mem_cache_reader DUCKDB_GUARDED_BY(mutex);
	unique_ptr<BaseCacheReader> on_disk_cache_reader DUCKDB_GUARDED_BY(mutex);
	BaseCacheReader *internal_cache_reader DUCKDB_GUARDED_BY(mutex) = nullptr;
};

//===--------------------------------------------------------------------===//
// Per-instance configuration
//===--------------------------------------------------------------------===//
struct InstanceConfig {
	// General config
	idx_t cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
	string cache_type = *DEFAULT_CACHE_TYPE;
	string profile_type = *DEFAULT_PROFILE_TYPE;
	uint64_t max_subrequest_count = DEFAULT_MAX_SUBREQUEST_COUNT;
	bool ignore_sigpipe = DEFAULT_IGNORE_SIGPIPE;

	// On-disk cache config
	vector<string> on_disk_cache_directories = {GetDefaultOnDiskCacheDirectory()};
	idx_t min_disk_bytes_for_cache = DEFAULT_MIN_DISK_BYTES_FOR_CACHE;
	string on_disk_eviction_policy = *DEFAULT_ON_DISK_EVICTION_POLICY;

	// Disk reader in-memory cache config
	bool enable_disk_reader_mem_cache = DEFAULT_ENABLE_DISK_READER_MEM_CACHE;
	idx_t disk_reader_max_mem_cache_block_count = DEFAULT_MAX_DISK_READER_MEM_CACHE_BLOCK_COUNT;
	idx_t disk_reader_max_mem_cache_timeout_millisec = DEFAULT_DISK_READER_MEM_CACHE_TIMEOUT_MILLISEC;

	// In-memory cache config
	idx_t max_in_mem_cache_block_count = DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT;
	idx_t in_mem_cache_block_timeout_millisec = DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC;

	// Metadata cache config
	bool enable_metadata_cache = DEFAULT_ENABLE_METADATA_CACHE;
	idx_t max_metadata_cache_entry = DEFAULT_MAX_METADATA_CACHE_ENTRY;
	idx_t metadata_cache_entry_timeout_millisec = DEFAULT_METADATA_CACHE_ENTRY_TIMEOUT_MILLISEC;

	// File handle cache config
	bool enable_file_handle_cache = DEFAULT_ENABLE_FILE_HANDLE_CACHE;
	idx_t max_file_handle_cache_entry = DEFAULT_MAX_FILE_HANDLE_CACHE_ENTRY;
	idx_t file_handle_cache_entry_timeout_millisec = DEFAULT_FILE_HANDLE_CACHE_ENTRY_TIMEOUT_MILLISEC;

	// Glob cache config
	bool enable_glob_cache = DEFAULT_ENABLE_GLOB_CACHE;
	idx_t max_glob_cache_entry = DEFAULT_MAX_GLOB_CACHE_ENTRY;
	idx_t glob_cache_entry_timeout_millisec = DEFAULT_GLOB_CACHE_ENTRY_TIMEOUT_MILLISEC;

	// Cache validation config
	bool enable_cache_validation = DEFAULT_ENABLE_CACHE_VALIDATION;

	// Cache invalidation on write config
	bool clear_cache_on_write = DEFAULT_CLEAR_CACHE_ON_WRITE;
};

//===--------------------------------------------------------------------===//
// Main per-instance state container
// Inherits from ObjectCacheEntry for automatic cleanup when DatabaseInstance is destroyed
//===--------------------------------------------------------------------===//
struct CacheHttpfsInstanceState : public ObjectCacheEntry {
	static constexpr const char *OBJECT_TYPE = "CacheHttpfsInstanceState";
	static constexpr const char *CACHE_KEY = "cache_httpfs_instance_state";

	// Extension config for the current duckdb instance.
	InstanceConfig config;
	// Cache filesystem registry.
	InstanceCacheFsRegistry registry;

	InstanceCacheReaderManager cache_reader_manager;
	CacheExclusionManager exclusion_manager;
	// Per-database profile collector, which is shared by all cache filesystems and cache readers.
	//
	// Thread-safety and ownership guarantee:
	// - Profile collector is a "singleton" owned by per-database cache httpfs instance state
	// - Both cache filesystems and cache readers make IO operation, so they hold a reference for profile collector to
	// record metrics
	// - Profile collector could be updated at extension setting update callback, which doesn't hold lock intentionally,
	// based on the assumption that setting update doesn't run concurrently with IO operation
	unique_ptr<BaseProfileCollector> profile_collector;

	CacheHttpfsInstanceState();

	// ObjectCacheEntry interface
	string GetObjectType() override {
		return OBJECT_TYPE;
	}

	static string ObjectType() {
		return OBJECT_TYPE;
	}

	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}

	// Get profile collector reference
	BaseProfileCollector &GetProfileCollector();
	// Reset profile collector.
	void ResetProfileCollector();
	// Set the profile collector.
	void SetProfileCollector(string profile_type);
};

//===--------------------------------------------------------------------===//
// Helper functions to access instance state
//===--------------------------------------------------------------------===//

// Store instance state in DatabaseInstance
void SetInstanceState(DatabaseInstance &instance, shared_ptr<CacheHttpfsInstanceState> state);

// Get instance state as shared_ptr from DatabaseInstance (returns nullptr if not set)
shared_ptr<CacheHttpfsInstanceState> GetInstanceStateShared(DatabaseInstance &instance);

// Get instance state, throwing if not found
CacheHttpfsInstanceState &GetInstanceStateOrThrow(DatabaseInstance &instance);

// Get instance state from ClientContext, throwing if not found
CacheHttpfsInstanceState &GetInstanceStateOrThrow(ClientContext &context);

// Get instance state as shared_ptr, throw exception if already unreferenced.
shared_ptr<CacheHttpfsInstanceState> GetInstanceConfig(weak_ptr<CacheHttpfsInstanceState> instance_state);

} // namespace duckdb

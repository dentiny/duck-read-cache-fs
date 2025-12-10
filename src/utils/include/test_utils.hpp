// This file contains test utility functions.

#pragma once

#include "cache_filesystem.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

// Test configuration options for creating test filesystems
struct TestCacheConfig {
	string cache_type = "noop";       // "noop", "on_disk", "in_mem"
	idx_t cache_block_size = 512_KiB; // Block size for caching
	string profile_type = "noop";     // "noop", "temp"
	vector<string> cache_directories = {"/tmp/duckdb_cache_httpfs_cache"};
	string eviction_policy = "creation_timestamp"; // Options: "creation_timestamp", "lru_sp"

	// Cache enable flags
	bool enable_metadata_cache = true;
	bool enable_file_handle_cache = true;
	bool enable_glob_cache = true;
	bool enable_disk_reader_mem_cache = true;
	bool enable_cache_validation = true;

	// Cache sizes
	idx_t max_metadata_cache_entry = 250;
	idx_t max_file_handle_cache_entry = 250;
	idx_t max_glob_cache_entry = 64;
	idx_t max_in_mem_cache_block_count = 8192;
	idx_t max_disk_reader_mem_cache_block_count = 8192;
	idx_t min_disk_bytes_for_cache = 0; // 0 means use default behavior
};

// Helper class to create a properly configured CacheFileSystem for testing.
// This creates a DuckDB instance with the extension state properly initialized.
class TestCacheFileSystemHelper {
public:
	explicit TestCacheFileSystemHelper(const TestCacheConfig &config = TestCacheConfig {});
	~TestCacheFileSystemHelper();

	// Get the cache filesystem for testing
	CacheFileSystem *GetCacheFileSystem() {
		return cache_fs.get();
	}

	// Get the underlying database instance
	DatabaseInstance &GetDatabaseInstance() {
		return *db.instance;
	}

	// Get the instance state
	CacheHttpfsInstanceState *GetInstanceState();

	// Get the config for inspection/modification
	InstanceConfig &GetConfig();

	// Get the profile collector for a specific connection (defaults to connection 0 for tests)
	BaseProfileCollector *GetProfileCollector(connection_t connection_id = 0);

private:
	DuckDB db;
	shared_ptr<CacheHttpfsInstanceState> instance_state;
	unique_ptr<CacheFileSystem> cache_fs;
};

} // namespace duckdb

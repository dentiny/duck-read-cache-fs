#include "test_utils.hpp"

#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

TestCacheFileSystemHelper::TestCacheFileSystemHelper(const TestCacheConfig &config) : db() {
	// Create and configure instance state
	instance_state = make_shared_ptr<CacheHttpfsInstanceState>();

	// Configure the instance
	auto &inst_config = instance_state->config;
	inst_config.cache_type = config.cache_type;
	inst_config.cache_block_size = config.cache_block_size;
	inst_config.profile_type = config.profile_type;
	inst_config.on_disk_cache_directories = config.cache_directories;
	inst_config.on_disk_eviction_policy = config.eviction_policy;

	// Cache enable flags
	inst_config.enable_metadata_cache = config.enable_metadata_cache;
	inst_config.enable_file_handle_cache = config.enable_file_handle_cache;
	inst_config.enable_glob_cache = config.enable_glob_cache;
	inst_config.enable_disk_reader_mem_cache = config.enable_disk_reader_mem_cache;

	// Cache sizes
	inst_config.max_metadata_cache_entry = config.max_metadata_cache_entry;
	inst_config.max_file_handle_cache_entry = config.max_file_handle_cache_entry;
	inst_config.max_glob_cache_entry = config.max_glob_cache_entry;
	inst_config.max_in_mem_cache_block_count = config.max_in_mem_cache_block_count;
	inst_config.disk_reader_max_mem_cache_block_count = config.max_disk_reader_mem_cache_block_count;
	inst_config.min_disk_bytes_for_cache = config.min_disk_bytes_for_cache;

	// Ensure cache directories exist
	auto local_fs = LocalFileSystem::CreateLocal();
	for (const auto &dir : inst_config.on_disk_cache_directories) {
		local_fs->CreateDirectory(dir);
	}

	// Register state with instance
	SetInstanceState(*db.instance.get(), instance_state);

	// Create cache filesystem wrapping local filesystem
	cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal(), instance_state);
}

TestCacheFileSystemHelper::~TestCacheFileSystemHelper() {
	// Clean up cache filesystem first
	cache_fs.reset();
}

CacheHttpfsInstanceState *TestCacheFileSystemHelper::GetInstanceState() {
	return instance_state.get();
}

InstanceConfig &TestCacheFileSystemHelper::GetConfig() {
	return instance_state->config;
}

BaseProfileCollector *TestCacheFileSystemHelper::GetProfileCollector(connection_t connection_id) {
	if (!instance_state) {
		return nullptr;
	}
	return instance_state->profile_collector_manager.GetProfileCollector(connection_id);
}

} // namespace duckdb

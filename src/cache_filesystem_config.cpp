#include "cache_filesystem_config.hpp"

#include <cstdint>
#include <utility>

#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

void SetGlobalConfig(optional_ptr<FileOpener> opener) {
	if (opener == nullptr) {
		// Testing cache type has higher priority than [g_cache_type].
		if (!g_test_cache_type.empty()) {
			g_cache_type = g_test_cache_type;
		}
		LocalFileSystem::CreateLocal()->CreateDirectory(g_on_disk_cache_directory);
		return;
	}

	Value val;

	// Check and update cache block size if necessary.
	FileOpener::TryGetCurrentSetting(opener, "cached_http_cache_block_size", val);
	g_cache_block_size = val.GetValue<uint64_t>();

	// Check and update cache type if necessary, only assign if setting valid.
	FileOpener::TryGetCurrentSetting(opener, "cached_http_type", val);
	auto cache_type_string = val.ToString();
	if (cache_type_string == ON_DISK_CACHE_TYPE || cache_type_string == IN_MEM_CACHE_TYPE) {
		g_cache_type = std::move(cache_type_string);
	}

	// Testing cache type has higher priority than [g_cache_type].
	if (!g_test_cache_type.empty()) {
		g_cache_type = g_test_cache_type;
	}

	// Check and update profile collector type if necessary, only assign if valid.
	FileOpener::TryGetCurrentSetting(opener, "cached_http_profile_type", val);
	auto profile_type_string = val.ToString();
	if (profile_type_string == NOOP_PROFILE_TYPE || profile_type_string == TEMP_PROFILE_TYPE ||
	    profile_type_string == PERSISTENT_PROFILE_TYPE) {
		g_profile_type = std::move(profile_type_string);
	}

	// Check and update configurations for on-disk cache type.
	if (g_cache_type == ON_DISK_CACHE_TYPE) {
		// Check and update cache directory if necessary.
		FileOpener::TryGetCurrentSetting(opener, "cached_http_cache_directory", val);
		auto new_on_disk_cache_directory = val.ToString();
		if (new_on_disk_cache_directory != g_on_disk_cache_directory) {
			g_on_disk_cache_directory = std::move(new_on_disk_cache_directory);
			LocalFileSystem::CreateLocal()->CreateDirectory(g_on_disk_cache_directory);
		}
	}

	// Check and update configurations for in-memory cache type.
	if (g_cache_type == IN_MEM_CACHE_TYPE) {
		// Check and update max cache block count.
		FileOpener::TryGetCurrentSetting(opener, "cached_http_max_in_mem_cache_block_count", val);
		g_max_in_mem_cache_block_count = val.GetValue<uint64_t>();
	}
}

void ResetGlobalConfig() {
	// Intentionally not set [g_test_cache_type].
	g_cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
	g_on_disk_cache_directory = DEFAULT_ON_DISK_CACHE_DIRECTORY;
	g_max_in_mem_cache_block_count = DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT;
	g_cache_type = DEFAULT_CACHE_TYPE;
	g_profile_type = DEFAULT_PROFILE_TYPE;
}

} // namespace duckdb

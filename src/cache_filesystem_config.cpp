#include "cache_filesystem_config.hpp"

#include <cstdint>
#include <csignal>
#include <utility>

#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

void SetGlobalConfig(optional_ptr<FileOpener> opener) {
	if (opener == nullptr) {
		// Testing cache type has higher priority than [g_cache_type].
		if (!g_test_cache_type->empty()) {
			*g_cache_type = *g_test_cache_type;
		}
		LocalFileSystem::CreateLocal()->CreateDirectory(*g_on_disk_cache_directory);
		return;
	}

	Value val;

	//===--------------------------------------------------------------------===//
	// Global cache configuration
	//===--------------------------------------------------------------------===//

	// Check and update cache type if necessary, only assign if setting valid.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_type", val);
	auto cache_type_string = val.ToString();
	if (ALL_CACHE_TYPES.find(cache_type_string) != ALL_CACHE_TYPES.end()) {
		*g_cache_type = std::move(cache_type_string);
	}

	// Testing cache type has higher priority than [g_cache_type].
	if (!g_test_cache_type->empty()) {
		*g_cache_type = *g_test_cache_type;
	}

	// Check and update cache block size if necessary.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_cache_block_size", val);
	const auto cache_block_size = val.GetValue<uint64_t>();
	if (cache_block_size > 0) {
		g_cache_block_size = cache_block_size;
	}

	// Check and update profile collector type if necessary, only assign if valid.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_profile_type", val);
	auto profile_type_string = val.ToString();
	if (ALL_PROFILE_TYPES->find(profile_type_string) != ALL_PROFILE_TYPES->end()) {
		*g_profile_type = std::move(profile_type_string);
	}

	// Check and update configuration for max subrequest count.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_max_fanout_subrequest", val);
	g_max_subrequest_count = val.GetValue<uint64_t>();

	// Check and update configurations to ignore SIGPIPE if necessary.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_ignore_sigpipe", val);
	const bool ignore_sigpipe = val.GetValue<bool>();
	if (!g_ignore_sigpipe && ignore_sigpipe) {
		g_ignore_sigpipe = true;
		// Ignore SIGPIPE, reference: https://blog.erratasec.com/2018/10/tcpip-sockets-and-sigpipe.html
		std::signal(SIGPIPE, SIG_IGN);
	}

	//===--------------------------------------------------------------------===//
	// On-disk cache configuration
	//===--------------------------------------------------------------------===//

	// Check and update configurations for on-disk cache type.
	if (*g_cache_type == *ON_DISK_CACHE_TYPE) {
		// Check and update cache directory if necessary.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_cache_directory", val);
		auto new_on_disk_cache_directory = val.ToString();
		if (new_on_disk_cache_directory != *g_on_disk_cache_directory) {
			*g_on_disk_cache_directory = std::move(new_on_disk_cache_directory);
			LocalFileSystem::CreateLocal()->CreateDirectory(*g_on_disk_cache_directory);
		}

		// Check and update min bytes for disk cache.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_min_disk_bytes_for_cache", val);
		const auto disk_cache_min_bytes = val.GetValue<uint64_t>();
		if (disk_cache_min_bytes > 0) {
			g_min_disk_bytes_for_cache = disk_cache_min_bytes;
		}
	}

	//===--------------------------------------------------------------------===//
	// In-mem cache configuration
	//===--------------------------------------------------------------------===//

	// Check and update configurations for in-memory cache type.
	if (*g_cache_type == *IN_MEM_CACHE_TYPE) {
		// Check and update max cache block count.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_max_in_mem_cache_block_count", val);
		const auto in_mem_block_count = val.GetValue<uint64_t>();
		if (in_mem_block_count > 0) {
			g_max_in_mem_cache_block_count = in_mem_block_count;
		}

		// Check and update in-memory data block caxche timeout.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_in_mem_cache_block_timeout_millisec", val);
		g_in_mem_cache_block_timeout_millisec = val.GetValue<uint64_t>();
	}

	//===--------------------------------------------------------------------===//
	// Metadata cache configuration
	//===--------------------------------------------------------------------===//

	// Check and update configurations for metadata cache enablement.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_enable_metadata_cache", val);
	g_enable_metadata_cache = val.GetValue<bool>();

	// Check and update metadata cache config if enabled.
	if (g_enable_metadata_cache) {
		// Check and update cache entry size.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_metadata_cache_entry_size", val);
		g_max_metadata_cache_entry = val.GetValue<uint64_t>();

		// Check and update cache entry timeout.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_metadata_cache_entry_timeout_millisec", val);
		g_metadata_cache_entry_timeout_millisec = val.GetValue<uint64_t>();
	}
}

void ResetGlobalConfig() {
	// Intentionally not set [g_test_cache_type] and [g_ignore_sigpipe].

	// Global configuration.
	g_cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
	*g_cache_type = *DEFAULT_CACHE_TYPE;
	*g_profile_type = *DEFAULT_PROFILE_TYPE;
	g_max_subrequest_count = DEFAULT_MAX_SUBREQUEST_COUNT;

	// On-disk cache configuration.
	*g_on_disk_cache_directory = *DEFAULT_ON_DISK_CACHE_DIRECTORY;
	g_min_disk_bytes_for_cache = DEFAULT_MIN_DISK_BYTES_FOR_CACHE;

	// In-memory cache configuration.
	g_max_in_mem_cache_block_count = DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT;
	g_in_mem_cache_block_timeout_millisec = DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC;

	// Metadata cache configuration.
	g_enable_metadata_cache = DEFAULT_ENABLE_METADATA_CACHE;
	g_max_metadata_cache_entry = DEFAULT_MAX_METADATA_CACHE_ENTRY;
	g_metadata_cache_entry_timeout_millisec = DEFAULT_METADATA_CACHE_ENTRY_TIMEOUT_MILLISEC;

	// Reset testing options.
	g_test_insufficient_disk_space = false;
}

uint64_t GetThreadCountForSubrequests(uint64_t io_request_count) {
	if (g_max_subrequest_count == 0) {
		return io_request_count;
	}
	return MinValue<uint64_t>(io_request_count, g_max_subrequest_count);
}

} // namespace duckdb

#include "cache_filesystem_config.hpp"

#include <cstdint>
#include <utility>

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

// Cache directories configs split token.s
constexpr char CACHE_DIRECTORIES_CONFIG_SPLITTER = ';';

// Parse directory configuration string into directories.
vector<string> ParseCacheDirectoryConfig(const std::string &directory_config_str) {
	auto directories = StringUtil::Split(directory_config_str, /*delimiter=*/CACHE_DIRECTORIES_CONFIG_SPLITTER);
	// Sort the cache directories, so for different directories config value with same directory sets, ordering doesn't
	// affect cache status.
	std::sort(directories.begin(), directories.end());
	return directories;
}

} // namespace

uint64_t GetThreadCountForSubrequests(uint64_t io_request_count, uint64_t max_subrequest_count) {
	if (max_subrequest_count == 0) {
		// Different platforms have different limits on the number of threads, use 1000 as the hard cap, above which
		// also increases context switch overhead.
		static constexpr uint64_t MAX_THREAD_COUNT = 1024;
		return MinValue<uint64_t>(io_request_count, MAX_THREAD_COUNT);
	}
	return MinValue<uint64_t>(io_request_count, max_subrequest_count);
}

std::vector<std::string> GetCacheDirectoryConfig(optional_ptr<FileOpener> opener) {
	Value val;

	// Attempt to get cache directories config first.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_cache_directories_config", val);
	auto new_cache_directories_config = val.ToString();
	if (!new_cache_directories_config.empty()) {
		// Cache directory parameter will be ignored, if directory config specified.
		auto directories = ParseCacheDirectoryConfig(new_cache_directories_config);
		return directories;
	}

	// Then fallback to parse cache directory.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_cache_directory", val);
	auto new_on_disk_cache_directory = val.ToString();
	vector<string> directories;
	directories.emplace_back(std::move(new_on_disk_cache_directory));
	return directories;
}

} // namespace duckdb

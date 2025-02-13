#define DUCKDB_EXTENSION_MAIN

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "crypto.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/extension_util.hpp"
#include "hffs.hpp"
#include "httpfs_extension.hpp"
#include "read_cache_fs_extension.hpp"
#include "s3fs.hpp"

#include <array>

namespace duckdb {

// Store all cache filesystems to access their profile collector.
// Lifecycle lies in virtual filesystem and db instance.
static vector<CacheFileSystem *> cache_file_systems;

static void ClearOnDiskCache(const DataChunk &args, ExpressionState &state, Vector &result) {
	for (auto *cur_filesystem : cache_file_systems) {
		cur_filesystem->ClearCache();
	}
	constexpr int32_t SUCCESS = 1;
	result.Reference(Value(SUCCESS));
}

static void GetOnDiskCacheSize(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto local_filesystem = LocalFileSystem::CreateLocal();

	int64_t total_cache_size = 0;
	local_filesystem->ListFiles(
	    g_on_disk_cache_directory, [&local_filesystem, &total_cache_size](const string &fname, bool /*unused*/) {
		    const string file_path = StringUtil::Format("%s/%s", g_on_disk_cache_directory, fname);
		    auto file_handle = local_filesystem->OpenFile(file_path, FileOpenFlags::FILE_FLAGS_READ);
		    total_cache_size += local_filesystem->GetFileSize(*file_handle);
	    });
	result.Reference(Value(total_cache_size));
}

static void GetProfileStats(const DataChunk &args, ExpressionState &state, Vector &result) {
	string latest_stat;
	uint64_t latest_timestamp = 0;
	for (auto *cur_filesystem : cache_file_systems) {
		auto *profile_collector = cur_filesystem->GetProfileCollector();
		// Profile collector is only initialized after cache filesystem access.
		if (profile_collector == nullptr) {
			continue;
		}

		auto [cur_profile_stat, cur_timestamp] = profile_collector->GetHumanReadableStats();
		if (cur_timestamp > latest_timestamp) {
			latest_timestamp = cur_timestamp;
			latest_stat = std::move(cur_profile_stat);
			continue;
		}
		if (cur_timestamp == latest_timestamp) {
			latest_stat = MaxValue<string>(latest_stat, cur_profile_stat);
		}
	}

	if (latest_stat.empty()) {
		latest_stat = "No valid access to cache filesystem";
	}
	result.Reference(Value(std::move(latest_stat)));
}

static void ResetProfileStats(const DataChunk &args, ExpressionState &state, Vector &result) {
	for (auto *cur_filesystem : cache_file_systems) {
		auto *profile_collector = cur_filesystem->GetProfileCollector();
		// Profile collector is only initialized after cache filesystem access.
		if (profile_collector == nullptr) {
			continue;
		}
		profile_collector->Reset();
	}

	constexpr int32_t SUCCESS = 1;
	result.Reference(Value(SUCCESS));
}

// Cached httpfs cannot co-exist with non-cached version, because duckdb virtual filesystem doesn't provide a native fs
// wrapper nor priority system, so co-existence doesn't guarantee cached version is actually used.
//
// Here's how we handled (a hacky way):
// 1. When we register cached filesystem, if uncached version already registered, we unregister them.
// 2. If uncached filesystem is registered later somehow, cached version is set mutual set so it has higher priority
// than uncached version.
static void LoadInternal(DatabaseInstance &instance) {
	// Register filesystem instance to instance.
	// Here we register both in-memory filesystem and on-disk filesystem, and leverage global configuration to decide
	// which one to use.
	auto &fs = instance.GetFileSystem();

	cache_file_systems.reserve(3);
	auto cached_http_filesystem = make_uniq<CacheFileSystem>(make_uniq<HTTPFileSystem>());
	cache_file_systems.emplace_back(cached_http_filesystem.get());
	fs.RegisterSubSystem(std::move(cached_http_filesystem));

	auto cached_hf_filesystem = make_uniq<CacheFileSystem>(make_uniq<HuggingFaceFileSystem>());
	cache_file_systems.emplace_back(cached_hf_filesystem.get());
	fs.RegisterSubSystem(std::move(cached_hf_filesystem));

	auto cached_s3_filesystem =
	    make_uniq<CacheFileSystem>(make_uniq<S3FileSystem>(BufferManager::GetBufferManager(instance)));
	cache_file_systems.emplace_back(cached_s3_filesystem.get());
	fs.RegisterSubSystem(std::move(cached_s3_filesystem));

	const std::array<string, 3> httpfs_names {"HTTPFileSystem", "S3FileSystem", "HuggingFaceFileSystem"};
	for (const auto &cur_http_fs : httpfs_names) {
		try {
			fs.UnregisterSubSystem(cur_http_fs);
		} catch (...) {
		}
	}

	// Register extension configuration.
	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption("cached_http_cache_directory", "The disk cache directory that stores cached data",
	                          LogicalType::VARCHAR, DEFAULT_ON_DISK_CACHE_DIRECTORY);
	config.AddExtensionOption(
	    "cached_http_cache_block_size",
	    "Block size for cache, applies to both in-memory cache filesystem and on-disk cache filesystem. It's worth "
	    "noting for on-disk filesystem, all existing cache files are invalidated after config update.",
	    LogicalType::UBIGINT, Value::UBIGINT(DEFAULT_CACHE_BLOCK_SIZE));
	config.AddExtensionOption("cached_http_max_in_mem_cache_block_count",
	                          "Max in-memory cache block count for in-memory cache filesystem. It's worth noting it "
	                          "should be set only once before all filesystem access, otherwise there's no affect.",
	                          LogicalType::UBIGINT, Value::UBIGINT(DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT));
	config.AddExtensionOption("cached_http_type",
	                          "Type for cached filesystem. Currently there're two types available, one is `in_mem`, "
	                          "another is `on_disk`. By default we use on-disk cache. Set to `noop` to disable, which "
	                          "behaves exactly same as httpfs extension.",
	                          LogicalType::VARCHAR, ON_DISK_CACHE_TYPE);
	config.AddExtensionOption(
	    "cached_http_profile_type",
	    "Profiling type for cached filesystem. There're three options available: `noop`, `temp`, and `duckdb`. `temp` "
	    "option stores the latest IO operation profiling result, which potentially suffers concurrent updates; "
	    "`duckdb` stores the IO operation profiling results into duckdb table, which unblocks advanced analysis.",
	    LogicalType::VARCHAR, DEFAULT_PROFILE_TYPE);

	// Register on-disk cache cleanup function.
	ScalarFunction clear_cache_function("cache_httpfs_clear_cache", /*arguments=*/ {},
	                                    /*return_type=*/LogicalType::INTEGER, ClearOnDiskCache);
	ExtensionUtil::RegisterFunction(instance, clear_cache_function);

	// Register on-disk cache file size stat function.
	ScalarFunction get_cache_size_function("cache_httpfs_get_cache_size", /*arguments=*/ {},
	                                       /*return_type=*/LogicalType::BIGINT, GetOnDiskCacheSize);
	ExtensionUtil::RegisterFunction(instance, get_cache_size_function);

	// Register profile collector metrics.
	ScalarFunction get_profile_stats_function("cache_httpfs_get_profile", /*arguments=*/ {},
	                                          /*return_type=*/LogicalType::VARCHAR, GetProfileStats);
	ExtensionUtil::RegisterFunction(instance, get_profile_stats_function);

	// Register profile collector metrics reset.
	ScalarFunction clear_profile_stats_function("cache_httpfs_clear_profile", /*arguments=*/ {},
	                                            /*return_type=*/LogicalType::BIGINT, ResetProfileStats);
	ExtensionUtil::RegisterFunction(instance, clear_profile_stats_function);
}

void ReadCacheFsExtension::Load(DuckDB &db) {
	// To achieve full compatibility for duckdb-httpfs extension, all related functions/types/... should be supported,
	// so we load it first.
	httpfs_extension = make_uniq<HttpfsExtension>();
	// It's possible httpfs is already loaded beforehand, simply capture exception and proceed.
	try {
		httpfs_extension->Load(db);
	} catch (...) {
	}

	// Load cached httpfs extension.
	LoadInternal(*db.instance);
}
std::string ReadCacheFsExtension::Name() {
	return "read_cache_fs";
}

std::string ReadCacheFsExtension::Version() const {
#ifdef EXT_VERSION_READ_CACHE_FS
	return EXT_VERSION_READ_CACHE_FS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void read_cache_fs_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ReadCacheFsExtension>();
}

DUCKDB_EXTENSION_API const char *read_cache_fs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

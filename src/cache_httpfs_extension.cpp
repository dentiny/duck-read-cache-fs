#define DUCKDB_EXTENSION_MAIN

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_filesystem_ref_registry.hpp"
#include "cache_httpfs_extension.hpp"
#include "cache_reader_manager.hpp"
#include "cache_status_query_function.hpp"
#include "crypto.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/main/extension_util.hpp"
#include "fake_filesystem.hpp"
#include "hffs.hpp"
#include "httpfs_extension.hpp"
#include "s3fs.hpp"

#include <array>

namespace duckdb {

// Current duckdb instance; store globally to retrieve filesystem instance inside of it.
static shared_ptr<DatabaseInstance> duckdb_instance;

// Clear both in-memory and on-disk data block cache.
static void ClearAllCache(const DataChunk &args, ExpressionState &state, Vector &result) {
	// Special handle local disk cache clear, since it's possible disk cache reader hasn't been initialized.
	auto local_filesystem = LocalFileSystem::CreateLocal();
	local_filesystem->RemoveDirectory(g_on_disk_cache_directory);
	local_filesystem->CreateDirectory(g_on_disk_cache_directory);

	// Clear cache for all initialized cache readers.
	CacheReaderManager::Get().ClearCache();

	constexpr bool SUCCESS = true;
	result.Reference(Value(SUCCESS));
}

static void ClearCacheForFile(const DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	const string fname = args.GetValue(/*col_idx=*/0, /*index=*/0).ToString();

	// Clear cache on the given [fname] for all initialized filesystems.
	CacheReaderManager::Get().ClearCache(fname);

	constexpr bool SUCCESS = true;
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
	const auto &cache_file_systems = CacheFsRefRegistry::Get().GetAllCacheFs();
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
	const auto &cache_file_systems = CacheFsRefRegistry::Get().GetAllCacheFs();
	for (auto *cur_filesystem : cache_file_systems) {
		auto *profile_collector = cur_filesystem->GetProfileCollector();
		// Profile collector is only initialized after cache filesystem access.
		if (profile_collector == nullptr) {
			continue;
		}
		profile_collector->Reset();
	}

	constexpr bool SUCCESS = true;
	result.Reference(Value(SUCCESS));
}

// Wrap the filesystem with extension cache filesystem.
// Throw exception if the requested filesystem hasn't been registered into duckdb instance.
static void WrapCacheFileSystem(const DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	const string filesystem_name = args.GetValue(/*col_idx=*/0, /*index=*/0).ToString();

	// duckdb instance has a opener filesystem, which is a wrapper around virtual filesystem.
	auto &opener_filesystem = duckdb_instance->GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	auto internal_filesystem = vfs.ExtractSubSystem(filesystem_name);
	if (internal_filesystem == nullptr) {
		throw InvalidInputException("Filesystem %s hasn't been registered yet!", filesystem_name);
	}
	vfs.RegisterSubSystem(make_uniq<CacheFileSystem>(std::move(internal_filesystem)));

	constexpr bool SUCCESS = true;
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
	// It's legal to reset database and reload extension, reset all global variable at load.
	CacheFsRefRegistry::Get().Reset();
	CacheReaderManager::Get().Reset();
	ResetGlobalConfig();

	// Register filesystem instance to instance.
	// Here we register both in-memory filesystem and on-disk filesystem, and leverage global configuration to decide
	// which one to use.
	auto &fs = instance.GetFileSystem();

	// TODO(hjiang): Register a fake filesystem at extension load for testing purpose. This is not ideal since
	// additional necessary instance is shipped in the extension. Local filesystem is not viable because it's not
	// registered in virtual filesystem. A better approach is find another filesystem not in httpfs extension.
	fs.RegisterSubSystem(make_uniq<CacheHttpfsFakeFileSystem>());

	auto cache_httpfs_filesystem = make_uniq<CacheFileSystem>(make_uniq<HTTPFileSystem>());
	CacheFsRefRegistry::Get().Register(cache_httpfs_filesystem.get());
	fs.RegisterSubSystem(std::move(cache_httpfs_filesystem));

	auto cached_hf_filesystem = make_uniq<CacheFileSystem>(make_uniq<HuggingFaceFileSystem>());
	CacheFsRefRegistry::Get().Register(cached_hf_filesystem.get());
	fs.RegisterSubSystem(std::move(cached_hf_filesystem));

	auto cached_s3_filesystem =
	    make_uniq<CacheFileSystem>(make_uniq<S3FileSystem>(BufferManager::GetBufferManager(instance)));
	CacheFsRefRegistry::Get().Register(cached_s3_filesystem.get());
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
	config.AddExtensionOption("cache_httpfs_cache_directory", "The disk cache directory that stores cached data",
	                          LogicalType::VARCHAR, DEFAULT_ON_DISK_CACHE_DIRECTORY);
	config.AddExtensionOption(
	    "cache_httpfs_cache_block_size",
	    "Block size for cache, applies to both in-memory cache filesystem and on-disk cache filesystem. It's worth "
	    "noting for on-disk filesystem, all existing cache files are invalidated after config update.",
	    LogicalType::UBIGINT, Value::UBIGINT(DEFAULT_CACHE_BLOCK_SIZE));
	config.AddExtensionOption("cache_httpfs_max_in_mem_cache_block_count",
	                          "Max in-memory cache block count for in-memory caches for all cache filesystems, so "
	                          "users are able to configure the maximum memory consumption. It's worth noting it "
	                          "should be set only once before all filesystem access, otherwise there's no affect.",
	                          LogicalType::UBIGINT, Value::UBIGINT(DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT));
	config.AddExtensionOption("cache_httpfs_type",
	                          "Type for cached filesystem. Currently there're two types available, one is `in_mem`, "
	                          "another is `on_disk`. By default we use on-disk cache. Set to `noop` to disable, which "
	                          "behaves exactly same as httpfs extension.",
	                          LogicalType::VARCHAR, ON_DISK_CACHE_TYPE);
	config.AddExtensionOption(
	    "cache_httpfs_profile_type",
	    "Profiling type for cached filesystem. There're three options available: `noop`, `temp`, and `duckdb`. `temp` "
	    "option stores the latest IO operation profiling result, which potentially suffers concurrent updates; "
	    "`duckdb` stores the IO operation profiling results into duckdb table, which unblocks advanced analysis.",
	    LogicalType::VARCHAR, DEFAULT_PROFILE_TYPE);
	config.AddExtensionOption(
	    "cache_httpfs_max_fanout_subrequest",
	    "Cached httpfs performs parallel request by splittng them into small request, with request size decided by "
	    "config [cache_httpfs_cache_block_size]. The setting limits the maximum request to issue for a single "
	    "filesystem read request. 0 means no limit, by default we set no limit.",
	    LogicalType::BIGINT, 0);
	config.AddExtensionOption("cache_httpfs_min_disk_bytes_for_cache",
	                          "Min number of bytes on disk for the cache filesystem to enable on-disk cache; if left "
	                          "bytes is less than the threshold, LRU based cache file eviction will be performed."
	                          "By default, 5% disk space will be reserved for other usage. When min disk bytes "
	                          "specified with a positive value, the default value will be overriden.",
	                          LogicalType::UBIGINT, 0);
	config.AddExtensionOption("cache_httpfs_enable_metadata_cache",
	                          "Whether metadata cache is enable for cache filesystem. By default enabled.",
	                          LogicalTypeId::BOOLEAN, DEFAULT_ENABLE_METADATA_CACHE);
	config.AddExtensionOption(
	    "cache_httpfs_ignore_sigpipe",
	    "Whether to ignore SIGPIPE for the extension. By default not ignored. Once ignored, it cannot be reverted.",
	    LogicalTypeId::BOOLEAN, DEFAULT_IGNORE_SIGPIPE);

	// Register cache cleanup function for both in-memory and on-disk cache.
	ScalarFunction clear_cache_function("cache_httpfs_clear_cache", /*arguments=*/ {},
	                                    /*return_type=*/LogicalType::BOOLEAN, ClearAllCache);
	ExtensionUtil::RegisterFunction(instance, clear_cache_function);

	// Register cache cleanup function for the given filename.
	ScalarFunction clear_cache_for_file_function("cache_httpfs_clear_cache_for_file",
	                                             /*arguments=*/ {LogicalType::VARCHAR},
	                                             /*return_type=*/LogicalType::BOOLEAN, ClearCacheForFile);
	ExtensionUtil::RegisterFunction(instance, clear_cache_for_file_function);

	// Register a function to wrap all duckdb-vfs-compatible filesystems. By default only httpfs filesystem instances
	// are wrapped. Usage for the target filesystem can be used as normal.
	//
	// Example usage:
	// D. LOAD azure;
	// -- Wrap filesystem with its name.
	// D. SELECT cache_httpfs_wrap_cache_filesystem('AzureBlobStorageFileSystem');
	ScalarFunction wrap_cache_filesystem_function("cache_httpfs_wrap_cache_filesystem",
	                                              /*arguments=*/ {LogicalTypeId::VARCHAR},
	                                              /*return_type=*/LogicalTypeId::BOOLEAN, WrapCacheFileSystem);
	ExtensionUtil::RegisterFunction(instance, wrap_cache_filesystem_function);

	// Register on-disk cache file size stat function.
	ScalarFunction get_cache_size_function("cache_httpfs_get_cache_size", /*arguments=*/ {},
	                                       /*return_type=*/LogicalType::BIGINT, GetOnDiskCacheSize);
	ExtensionUtil::RegisterFunction(instance, get_cache_size_function);

	// Register on-disk cache file display.
	ExtensionUtil::RegisterFunction(instance, GetCacheStatusQueryFunc());

	// Register profile collector metrics.
	// A commonly-used SQL is `COPY (SELECT cache_httpfs_get_profile()) TO '/tmp/output.txt';`.
	ScalarFunction get_profile_stats_function("cache_httpfs_get_profile", /*arguments=*/ {},
	                                          /*return_type=*/LogicalType::VARCHAR, GetProfileStats);
	ExtensionUtil::RegisterFunction(instance, get_profile_stats_function);

	// Register profile collector metrics reset.
	ScalarFunction clear_profile_stats_function("cache_httpfs_clear_profile", /*arguments=*/ {},
	                                            /*return_type=*/LogicalType::BOOLEAN, ResetProfileStats);
	ExtensionUtil::RegisterFunction(instance, clear_profile_stats_function);

	// Create default cache directory.
	LocalFileSystem::CreateLocal()->CreateDirectory(DEFAULT_ON_DISK_CACHE_DIRECTORY);
}

void CacheHttpfsExtension::Load(DuckDB &db) {
	// To achieve full compatibility for duckdb-httpfs extension, all related functions/types/... should be supported,
	// so we load it first.
	httpfs_extension = make_uniq<HttpfsExtension>();
	// It's possible httpfs is already loaded beforehand, simply capture exception and proceed.
	try {
		httpfs_extension->Load(db);
	} catch (...) {
	}

	// Load cached httpfs extension.
	duckdb_instance = db.instance;
	LoadInternal(*db.instance);
}
std::string CacheHttpfsExtension::Name() {
	return "cache_httpfs";
}

std::string CacheHttpfsExtension::Version() const {
#ifdef EXT_VERSION_CACHE_HTTPFS
	return EXT_VERSION_CACHE_HTTPFS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void cache_httpfs_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::CacheHttpfsExtension>();
}

DUCKDB_EXTENSION_API const char *cache_httpfs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

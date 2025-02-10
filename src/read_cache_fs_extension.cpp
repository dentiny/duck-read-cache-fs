#define DUCKDB_EXTENSION_MAIN

#include "read_cache_fs_extension.hpp"
#include "disk_cache_filesystem.hpp"
#include "s3fs.hpp"
#include "hffs.hpp"
#include "crypto.hpp"
#include "duckdb/main/extension_util.hpp"
#include "cache_filesystem_config.hpp"

#include <array>

namespace duckdb {

static void ClearOnDiskCache(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	local_filesystem->RemoveDirectory(g_on_disk_cache_directory);
	// Create an empty directory, otherwise later read access errors.
	local_filesystem->CreateDirectory(g_on_disk_cache_directory);

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

// Cached httpfs cannot co-exist with non-cached version, because duckdb virtual
// filesystem doesn't provide a native fs wrapper nor priority system, so
// co-existence doesn't guarantee cached version is actually used.
//
// Here's how we handled (a hacky way):
// 1. When we register cached filesystem, if uncached version already
// registered, we unregister them.
// 2. If uncached filesystem is registered later somehow, cached version is set
// mutual set so it has higher priority than uncached version.
static void LoadInternal(DatabaseInstance &instance) {
	// Register filesystem instance to instance.
	auto &fs = instance.GetFileSystem();
	fs.RegisterSubSystem(make_uniq<DiskCacheFileSystem>(make_uniq<HTTPFileSystem>()));
	fs.RegisterSubSystem(make_uniq<DiskCacheFileSystem>(make_uniq<HuggingFaceFileSystem>()));
	fs.RegisterSubSystem(
	    make_uniq<DiskCacheFileSystem>(make_uniq<S3FileSystem>(BufferManager::GetBufferManager(instance))));

	const std::array<string, 3> httpfs_names {"HTTPFileSystem", "S3FileSystem", "HuggingFaceFileSystem"};
	for (const auto &cur_http_fs : httpfs_names) {
		try {
			fs.UnregisterSubSystem(cur_http_fs);
		} catch (...) {
		}
	}

	// Register extension configuration.
	//
	// TODO(hjiang): We should have a way to set global configurations to decide
	// cache mode.
	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption("cached_http_cache_directory", "The disk cache directory that stores cached data",
	                          LogicalType::VARCHAR, DEFAULT_ON_DISK_CACHE_DIRECTORY);
	config.AddExtensionOption("cached_http_cache_block_size",
	                          "Block size for cache, applies to both in-memory "
	                          "cache filesystem and on-disk cache filesystem. It's worth noting for "
	                          "on-disk filesystem, all existing cache files are invalidated after "
	                          "config update.",
	                          LogicalType::UBIGINT, Value::UBIGINT(DEFAULT_CACHE_BLOCK_SIZE));
	config.AddExtensionOption("cached_http_max_in_mem_cache_block_count",
	                          "Max in-memory cache block count for in-memory cache filesystem. It's "
	                          "worth noting it should be set only once before all filesystem access, "
	                          "otherwise there's no affect.",
	                          LogicalType::UBIGINT, Value::UBIGINT(DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT));

	// Register on-disk cache cleanup function.
	ScalarFunction clear_cache_function("cache_httpfs_clear_cache", /*arguments=*/ {},
	                                    /*return_type=*/LogicalType::INTEGER, ClearOnDiskCache);
	ExtensionUtil::RegisterFunction(instance, clear_cache_function);

	// Register on-disk cache file size stat function.
	ScalarFunction get_cache_size_function("cache_httpfs_get_cache_size", /*arguments=*/ {},
	                                       /*return_type=*/LogicalType::BIGINT, GetOnDiskCacheSize);
	ExtensionUtil::RegisterFunction(instance, get_cache_size_function);
}

void ReadCacheFsExtension::Load(DuckDB &db) {
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

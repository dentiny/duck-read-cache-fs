#define DUCKDB_EXTENSION_MAIN

#include "read_cache_fs_extension.hpp"
#include "disk_cache_filesystem.hpp"
#include "s3fs.hpp"
#include "hffs.hpp"
#include "crypto.hpp"

#include <array>

namespace duckdb {

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
  auto &fs = instance.GetFileSystem();
  fs.RegisterSubSystem(
      make_uniq<DiskCacheFileSystem>(make_uniq<HTTPFileSystem>()));
  fs.RegisterSubSystem(
      make_uniq<DiskCacheFileSystem>(make_uniq<HuggingFaceFileSystem>()));
  fs.RegisterSubSystem(make_uniq<DiskCacheFileSystem>(
      make_uniq<S3FileSystem>(BufferManager::GetBufferManager(instance))));

  const std::array<string, 3> httpfs_names{"HTTPFileSystem", "S3FileSystem",
                                           "HuggingFaceFileSystem"};
  for (const auto &cur_http_fs : httpfs_names) {
    try {
      fs.UnregisterSubSystem(cur_http_fs);
    } catch (...) {
    }
  }
}

void ReadCacheFsExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string ReadCacheFsExtension::Name() { return "read_cache_fs"; }

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

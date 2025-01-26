#define DUCKDB_EXTENSION_MAIN

#include "read_cache_fs_extension.hpp"
#include "disk_cache_filesystem.hpp"
#include "s3fs.hpp"
#include "hffs.hpp"
#include "crypto.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
  // TODO(hjiang): We cannot have both cached remote filesystem (i.e. httpfs)
  // and remote fs in the same vfs.
  auto &fs = instance.GetFileSystem();
  fs.RegisterSubSystem(
      make_uniq<DiskCacheFileSystem>(make_uniq<HTTPFileSystem>()));
  fs.RegisterSubSystem(
      make_uniq<DiskCacheFileSystem>(make_uniq<HuggingFaceFileSystem>()));
  fs.RegisterSubSystem(make_uniq<DiskCacheFileSystem>(
      make_uniq<S3FileSystem>(BufferManager::GetBufferManager(instance))));
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

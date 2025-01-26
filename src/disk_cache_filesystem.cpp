#include "disk_cache_filesystem.hpp"

#include <utility>

namespace duckdb {

DiskCacheFileHandle::DiskCacheFileHandle(
    unique_ptr<FileHandle> internal_file_handle_p, DiskCacheFileSystem &fs)
    : FileHandle(fs, internal_file_handle_p->GetPath(),
                 internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {}

DiskCacheFileSystem::DiskCacheFileSystem(
    unique_ptr<FileSystem> internal_filesystem_p)
    : internal_filesystem(std::move(internal_filesystem_p)) {}

} // namespace duckdb

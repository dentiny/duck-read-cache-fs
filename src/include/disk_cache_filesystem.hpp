// A filesystem wrapper, which performs on-disk cache for read operations.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/file_opener.hpp"
#include "cache_filesystem_config.hpp"
#include "base_cache_filesystem.hpp"

namespace duckdb {

class DiskCacheFileSystem final : public CacheFileSystem {
public:
  explicit DiskCacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p)
      : DiskCacheFileSystem(std::move(internal_filesystem_p),
                            OnDiskCacheConfig{}) {}
  DiskCacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p,
                      OnDiskCacheConfig cache_directory_p);
  std::string GetName() const override;

  unique_ptr<FileHandle>
  OpenFile(const string &path, FileOpenFlags flags,
           optional_ptr<FileOpener> opener = nullptr) override;

protected:
  // Read from [handle] for an block-size aligned chunk into [start_addr]; cache
  // to local filesystem and return to user.
  void ReadAndCache(FileHandle &handle, char *buffer,
                    idx_t requested_start_offset, idx_t requested_bytes_to_read,
                    idx_t file_size) override;

  // Read-cache filesystem configuration.
  OnDiskCacheConfig cache_config;
  // Directory to store cache files.
  string cache_directory;
  // Used to access local cache files.
  unique_ptr<FileSystem> local_filesystem;
};

} // namespace duckdb

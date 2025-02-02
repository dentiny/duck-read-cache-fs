// A filesystem wrapper, which performs on-disk cache for read operations.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
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
  std::string GetName() const override { return "disk_cache_filesystem"; }

protected:
  // Read from [handle] for an block-size aligned chunk into [start_addr]; cache
  // to local filesystem and return to user.
  void ReadAndCache(FileHandle &handle, char *buffer,
                    uint64_t requested_start_offset,
                    uint64_t requested_bytes_to_read,
                    uint64_t file_size) override;

  // Read-cache filesystem configuration.
  OnDiskCacheConfig cache_config;
  // Directory to store cache files.
  string cache_directory;
  // Used to access local cache files.
  unique_ptr<FileSystem> local_filesystem;
};

} // namespace duckdb

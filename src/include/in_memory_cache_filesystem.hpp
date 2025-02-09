// A filesystem wrapper, which performs in-memory cache for read operations.
//
// TODO(hjiang): The implementation for parallel read, check and update cache is
// basically the same, should extract into a comon function.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "cache_filesystem_config.hpp"
#include "in_mem_cache_block.hpp"
#include "lru_cache.hpp"
#include "base_cache_filesystem.hpp"
#include "duckdb/common/file_opener.hpp"

namespace duckdb {

class InMemoryCacheFileSystem : public CacheFileSystem {
public:
  explicit InMemoryCacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p)
      : InMemoryCacheFileSystem(std::move(internal_filesystem_p),
                                InMemoryCacheConfig{}) {}
  InMemoryCacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p,
                          InMemoryCacheConfig cache_config);
  std::string GetName() const override;

  unique_ptr<FileHandle>
  OpenFile(const string &path, FileOpenFlags flags,
           optional_ptr<FileOpener> opener = nullptr) override;

protected:
  using InMemCache =
      ThreadSafeSharedLruCache<InMemCacheBlock, string, InMemCacheBlockHash,
                               InMemCacheBlockEqual>;

  // Read from [handle] for an block-size aligned chunk into [start_addr]; cache
  // to local filesystem and return to user.
  void ReadAndCache(FileHandle &handle, char *buffer,
                    uint64_t requested_start_offset,
                    uint64_t requested_bytes_to_read,
                    uint64_t file_size) override;

  // Read-cache filesystem configuration.
  InMemoryCacheConfig cache_config;
  // LRU cache to store blocks; late initialized after first access.
  unique_ptr<InMemCache> cache;
};

} // namespace duckdb

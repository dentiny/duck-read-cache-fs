#include "in_memory_cache_filesystem.hpp"
#include "duckdb/common/thread.hpp"
#include "crypto.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "utils/include/resize_uninitialized.hpp"
#include "utils/include/filesystem_utils.hpp"

#include <cstdint>
#include <utility>
#include <utime.h>

namespace duckdb {

namespace {

// All read requests are split into chunks, and executed in parallel.
// A [CacheReadChunk] represents a chunked IO request and its corresponding
// partial IO request.
struct CacheReadChunk {
  // Requested memory address and file offset to read from for current chunk.
  char *requested_start_addr = nullptr;
  uint64_t requested_start_offset = 0;
  // Block size aligned [requested_start_offset].
  uint64_t aligned_start_offset = 0;

  // Number of bytes for the chunk for IO operations, apart from the last chunk
  // it's always cache block size.
  uint64_t chunk_size = 0;

  // Number of bytes to copy from [content] to requested memory address.
  uint64_t bytes_to_copy = 0;

  // Copy from [content] to application-provided buffer.
  void CopyBufferToRequestedMemory(const std::string &content) {
    const uint64_t delta_offset = requested_start_offset - aligned_start_offset;
    std::memmove(requested_start_addr,
                 const_cast<char *>(content.data()) + delta_offset,
                 bytes_to_copy);
  }
};

} // namespace

InMemoryCacheFileHandle::InMemoryCacheFileHandle(
    unique_ptr<FileHandle> internal_file_handle_p, InMemoryCacheFileSystem &fs)
    : FileHandle(fs, internal_file_handle_p->GetPath(),
                 internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {}

InMemoryCacheFileSystem::InMemoryCacheFileSystem(
    unique_ptr<FileSystem> internal_filesystem_p,
    InMemoryCacheConfig cache_config_p)
    : cache_config(std::move(cache_config_p)),
      internal_filesystem(std::move(internal_filesystem_p)),
      cache(cache_config.block_count) {}

void InMemoryCacheFileSystem::Read(FileHandle &handle, void *buffer,
                                   int64_t nr_bytes, idx_t location) {
  ReadImpl(handle, buffer, nr_bytes, location, DEFAULT_BLOCK_SIZE);
}
int64_t InMemoryCacheFileSystem::Read(FileHandle &handle, void *buffer,
                                      int64_t nr_bytes) {
  const int64_t bytes_read = ReadImpl(
      handle, buffer, nr_bytes, handle.SeekPosition(), DEFAULT_BLOCK_SIZE);
  handle.Seek(handle.SeekPosition() + bytes_read);
  return bytes_read;
}
int64_t InMemoryCacheFileSystem::ReadForTesting(FileHandle &handle,
                                                void *buffer, int64_t nr_bytes,
                                                idx_t location,
                                                uint64_t block_size) {
  return ReadImpl(handle, buffer, nr_bytes, location, block_size);
}

void InMemoryCacheFileSystem::ReadAndCache(FileHandle &handle, char *buffer,
                                           uint64_t requested_start_offset,
                                           uint64_t requested_bytes_to_read,
                                           uint64_t file_size,
                                           uint64_t block_size) {
  const uint64_t aligned_start_offset =
      requested_start_offset / block_size * block_size;
  const uint64_t aligned_last_chunk_offset =
      (requested_start_offset + requested_bytes_to_read) / block_size *
      block_size;

  // Indicate the meory address to copy to for each IO operation
  char *addr_to_write = buffer;
  // Used to calculate bytes to copy for last chunk.
  uint64_t already_read_bytes = 0;
  // Threads to parallelly perform IO.
  vector<thread> io_threads;

  // To improve IO performance, we split requested bytes (after alignment) into
  // multiple chunks and fetch them in parallel.
  for (uint64_t io_start_offset = aligned_start_offset;
       io_start_offset <= aligned_last_chunk_offset;
       io_start_offset += block_size) {
    CacheReadChunk cache_read_chunk;
    cache_read_chunk.requested_start_addr = addr_to_write;
    cache_read_chunk.aligned_start_offset = io_start_offset;
    cache_read_chunk.requested_start_offset = requested_start_offset;

    // Implementation-wise, middle chunks are easy to handle -- read in
    // [block_size], and copy the whole chunk to the requested memory address;
    // but the first and last chunk require special handling.
    // For first chunk, requested start offset might not be aligned with block
    // size; for the last chunk, we might not need to copy the whole
    // [block_size] of memory.
    //
    // Case-1: If there's only one chunk, which serves as both the first chunk
    // and the last one.
    if (io_start_offset == aligned_start_offset &&
        io_start_offset == aligned_last_chunk_offset) {
      cache_read_chunk.chunk_size =
          std::min<uint64_t>(block_size, file_size - io_start_offset);
      cache_read_chunk.bytes_to_copy = requested_bytes_to_read;
    }
    // Case-2: First chunk.
    else if (io_start_offset == aligned_start_offset) {
      const uint64_t delta_offset =
          requested_start_offset - aligned_start_offset;
      addr_to_write += block_size - delta_offset;
      already_read_bytes += block_size - delta_offset;

      cache_read_chunk.chunk_size = block_size;
      cache_read_chunk.bytes_to_copy = block_size - delta_offset;
    }
    // Case-3: Last chunk.
    else if (io_start_offset == aligned_last_chunk_offset) {
      cache_read_chunk.chunk_size =
          std::min<uint64_t>(block_size, file_size - io_start_offset);
      cache_read_chunk.bytes_to_copy =
          requested_bytes_to_read - already_read_bytes;
    }
    // Case-4: Middle chunks.
    else {
      addr_to_write += block_size;
      already_read_bytes += block_size;

      cache_read_chunk.bytes_to_copy = block_size;
      cache_read_chunk.chunk_size = block_size;
    }

    // Update read offset for next chunk read.
    requested_start_offset = io_start_offset + block_size;

    // Perform read operation in parallel.
    io_threads.emplace_back(
        [this, &handle, block_size,
         cache_read_chunk = std::move(cache_read_chunk)]() mutable {
          // Check local cache first, see if we could do a cached read.
          InMemCacheBlock block_key;
          block_key.fname = handle.GetPath();
          block_key.start_off = cache_read_chunk.aligned_start_offset;
          block_key.blk_size = cache_read_chunk.chunk_size;
          string block_key_str = block_key.ToString();
          auto cache_block = cache.Get(block_key_str);

          // TODO(hjiang): Add documentation and implementation for stale cache
          // eviction policy, before that it's safe to access cache file
          // directly.
          if (cache_block != nullptr) {
            cache_read_chunk.CopyBufferToRequestedMemory(*cache_block);
            return;
          }

          // We suffer a cache loss, fallback to remote access then local
          // filesystem write.
          auto content =
              CreateResizeUninitializedString(cache_read_chunk.chunk_size);
          auto &in_mem_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
          internal_filesystem->Read(*in_mem_cache_handle.internal_file_handle,
                                    const_cast<char *>(content.data()),
                                    content.length(),
                                    cache_read_chunk.aligned_start_offset);

          // Copy to destination buffer.
          cache_read_chunk.CopyBufferToRequestedMemory(content);

          // Attempt to cache file locally.
          cache.Put(std::move(block_key_str),
                    std::make_shared<std::string>(std::move(content)));
        });
  }
  for (auto &cur_thd : io_threads) {
    D_ASSERT(cur_thd.joinable());
    cur_thd.join();
  }
}

int64_t InMemoryCacheFileSystem::ReadImpl(FileHandle &handle, void *buffer,
                                          int64_t nr_bytes, idx_t location,
                                          uint64_t block_size) {
  const auto file_size = handle.GetFileSize();

  // No more bytes to read.
  if (location == file_size) {
    return 0;
  }

  const int64_t bytes_to_read =
      std::min<int64_t>(nr_bytes, file_size - location);
  ReadAndCache(handle, static_cast<char *>(buffer), location, bytes_to_read,
               file_size, block_size);

  return bytes_to_read;
}

} // namespace duckdb

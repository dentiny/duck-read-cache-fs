#include "duckdb/common/thread.hpp"
#include "disk_cache_filesystem.hpp"
#include "crypto.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "resize_uninitialized.h"

#include <cstdint>
#include <utility>

namespace duckdb {

namespace {

struct DataChunk {
  char *requested_start_addr = nullptr;
  uint64_t requested_bytes_to_read = 0;

  // |             buffer                  |
  // | start-unreq-sz | req | end-unreq-sz |
  uint64_t start_unrequested_size = 0;

  // All IO operations are aligned with block size, if requested [start_addr]
  // isn't aligned with block size, allocate an internal buffer then copy to
  // [start_addr], otherwise it's unassigned.
  string buffer;

  char *GetAddrReadTo() {
    return !buffer.empty() ? const_cast<char *>(buffer.data())
                           : requested_start_addr;
  }
  uint64_t GetLenToRead() {
    return !buffer.empty() ? buffer.length() : requested_bytes_to_read;
  }
  void CopyBufferToRequestedMemory() {
    if (!buffer.empty()) {
      char *src = const_cast<char *>(buffer.data()) + start_unrequested_size;
      std::memmove(requested_start_addr, src, requested_bytes_to_read);
    }
  }
};

// IO operations are performed based on block size, this function returns
// aligned start offset and bytes to read.
std::pair<uint64_t, int64_t> GetStartOffsetAndBytesToRead(FileHandle &handle,
                                                          uint64_t start_offset,
                                                          int64_t bytes_to_read,
                                                          uint64_t file_size,
                                                          uint64_t block_size) {
  const uint64_t aligned_start_offset = start_offset / block_size * block_size;
  uint64_t aligned_end_offset =
      ((start_offset + bytes_to_read - 1) / block_size + 1) * block_size;
  aligned_end_offset = std::min<uint64_t>(aligned_end_offset, file_size);
  return std::make_pair(aligned_start_offset,
                        aligned_end_offset - aligned_start_offset);
}

// Convert SHA256 value to hex string.
string Sha256ToHexString(const duckdb::hash_bytes &sha256) {
  static constexpr char kHexChars[] = "0123456789abcdef";
  std::string result;
  // SHA256 has 32 byte, we encode 2 chars for each byte of SHA256.
  result.reserve(64);

  for (unsigned char byte : sha256) {
    result += kHexChars[byte >> 4];  // Get high 4 bits
    result += kHexChars[byte & 0xF]; // Get low 4 bits
  }
  return result;
}

// Get local cache filename for the given [remote_file].
//
// Cache filename is formatted as
// `<cache-directory>/<filename-sha256>.<filename>`. So we could get all cache
// files under one directory, and get all cache files with commands like `ls`.
//
// Considering the naming format, it's worth noting it might _NOT_ work for
// local files, including mounted filesystems.
string GetLocalCacheFile(const string &cache_directory,
                         const string &remote_file, uint64_t start_offset,
                         uint64_t bytes_to_read) {
  duckdb::hash_bytes remote_file_sha256_val;
  duckdb::sha256(remote_file.data(), remote_file.length(),
                 remote_file_sha256_val);
  const string remote_file_sha256_str =
      Sha256ToHexString(remote_file_sha256_val);

  const string fname = StringUtil::GetFileName(remote_file);
  return StringUtil::Format("%s/%s.%s-%llu-%llu", cache_directory,
                            remote_file_sha256_str, fname, start_offset,
                            bytes_to_read);
}

} // namespace

DiskCacheFileHandle::DiskCacheFileHandle(
    unique_ptr<FileHandle> internal_file_handle_p, DiskCacheFileSystem &fs)
    : FileHandle(fs, internal_file_handle_p->GetPath(),
                 internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {}

DiskCacheFileSystem::DiskCacheFileSystem(
    unique_ptr<FileSystem> internal_filesystem_p, string cache_directory_p)
    : cache_directory(std::move(cache_directory_p)),
      local_filesystem(FileSystem::CreateLocal()),
      internal_filesystem(std::move(internal_filesystem_p)) {
  local_filesystem->CreateDirectory(cache_directory);
}

void DiskCacheFileSystem::Read(FileHandle &handle, void *buffer,
                               int64_t nr_bytes, idx_t location) {
  ReadImpl(handle, buffer, nr_bytes, location, DEFAULT_BLOCK_SIZE);
}
int64_t DiskCacheFileSystem::Read(FileHandle &handle, void *buffer,
                                  int64_t nr_bytes) {
  const int64_t bytes_read = ReadImpl(
      handle, buffer, nr_bytes, handle.SeekPosition(), DEFAULT_BLOCK_SIZE);
  handle.Seek(handle.SeekPosition() + bytes_read);
  return bytes_read;
}
int64_t DiskCacheFileSystem::ReadForTesting(FileHandle &handle, void *buffer,
                                            int64_t nr_bytes, idx_t location,
                                            uint64_t block_size) {
  return ReadImpl(handle, buffer, nr_bytes, location, block_size);
}

void DiskCacheFileSystem::ReadAndCache(
    FileHandle &handle, char *buffer, uint64_t requested_start_offset,
    uint64_t requested_bytes_to_read, uint64_t aligned_start_offset,
    uint64_t aligned_bytes_to_read, uint64_t file_size, uint64_t block_size) {
  D_ASSERT(aligned_bytes_to_read >= 1);
  const uint64_t io_op_count = (aligned_bytes_to_read - 1) / block_size + 1;
  vector<thread> threads;
  threads.reserve(io_op_count);

  // |     buffer    | ...
  // | un-req | req  | ...
  const uint64_t offset_delta = requested_start_offset - aligned_start_offset;
  char *aligned_start_addr = buffer - offset_delta;

  for (uint64_t idx = 0; idx < io_op_count; ++idx) {
    DataChunk cur_data_chunk;
    cur_data_chunk.requested_start_addr = aligned_start_addr + idx * block_size;
    cur_data_chunk.requested_bytes_to_read = block_size;

    // Adjust first and last block.
    if (idx == 0 && idx == io_op_count - 1) {
      const uint64_t first_chunk_read_size =
          std::min(block_size, file_size - aligned_start_offset); // 1222
      cur_data_chunk.buffer =
          CreateResizeUninitializedString(first_chunk_read_size);
      cur_data_chunk.requested_start_addr = buffer;
      cur_data_chunk.requested_bytes_to_read = requested_bytes_to_read;
      cur_data_chunk.start_unrequested_size = offset_delta;
    }

    // Adjust first but not last block.
    if (idx == 0 && idx != io_op_count - 1) {
      const uint64_t first_chunk_read_size =
          std::min(block_size, file_size - aligned_start_offset);
      cur_data_chunk.buffer =
          CreateResizeUninitializedString(first_chunk_read_size);
      cur_data_chunk.requested_start_addr = buffer;
      cur_data_chunk.requested_bytes_to_read = block_size - offset_delta;
      cur_data_chunk.start_unrequested_size = offset_delta;
    }

    // Adjust last but not first block.
    if (idx == io_op_count - 1 && idx != 0) {
      const uint64_t last_chunk_start_offset =
          aligned_start_offset + block_size * (io_op_count - 1);
      const uint64_t last_chunk_end_offset =
          last_chunk_start_offset + block_size;

      // | un-req | req | ... | req | un-req |
      //          |<-  requested  ->|
      cur_data_chunk.requested_bytes_to_read = requested_bytes_to_read +
                                               offset_delta -
                                               (io_op_count - 1) * block_size;

      // Handle case-1: buffer end already exceeds file end
      // ... |      buffer     |
      // ... | file end | noth |
      // ... | req |   noth    |
      if (last_chunk_end_offset > file_size) {
        cur_data_chunk.buffer = CreateResizeUninitializedString(
            file_size - last_chunk_start_offset);
      }

      // Handle case-2.
      // ... |    buffer    |
      // ... |      ...     | ... | file end |
      // ... | req | un-req |
      else {
        cur_data_chunk.buffer = CreateResizeUninitializedString(block_size);
      }
    }

    if (idx != 0 && idx != io_op_count - 1)
      // Start file offset for current read operation.
      const uint64_t cur_start_offset = block_size * idx;

    threads.emplace_back([this, &handle, cur_start_offset,
                          cur_data_chunk =
                              std::move(cur_data_chunk)]() mutable {
      // Check local cache first, see if we could do a cached read.
      const auto local_cache_file =
          GetLocalCacheFile(cache_directory, handle.GetPath(), cur_start_offset,
                            cur_data_chunk.GetLenToRead());

      // TODO(hjiang): Add documentation and implementation for stale cache
      // eviction policy, before that it's safe to access cache file
      // directly.
      if (local_filesystem->FileExists(local_cache_file)) {
        auto file_handle = local_filesystem->OpenFile(
            local_cache_file, FileOpenFlags::FILE_FLAGS_READ);
        local_filesystem->Read(*file_handle, cur_data_chunk.GetAddrReadTo(),
                               cur_data_chunk.GetLenToRead(),
                               /*location=*/0);
        cur_data_chunk.CopyBufferToRequestedMemory();
        return;
      }

      // We suffer a cache loss, fallback to remote access then local
      // filesystem write.
      auto &disk_cache_handle = handle.Cast<DiskCacheFileHandle>();
      internal_filesystem->Read(*disk_cache_handle.internal_file_handle,
                                cur_data_chunk.GetAddrReadTo(),
                                cur_data_chunk.GetLenToRead(),
                                cur_start_offset);

      // TODO(hjiang): Before local cache we should check whether there's
      // enough space left, and trigger a stale file cleanup if necessary.
      //
      // Dump to a temporary location at local filesystem.
      const auto fname = StringUtil::GetFileName(handle.GetPath());
      const auto local_temp_file =
          StringUtil::Format("%s%s.%s.httpfs_local_cache", cache_directory,
                             fname, UUID::ToString(UUID::GenerateRandomUUID()));
      {
        auto file_handle = local_filesystem->OpenFile(
            local_temp_file, FileOpenFlags::FILE_FLAGS_WRITE |
                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
        local_filesystem->Write(*file_handle, cur_data_chunk.GetAddrReadTo(),
                                cur_data_chunk.GetLenToRead(),
                                /*location=*/0);
        file_handle->Sync();
      }

      // Then atomically move to the target postion to prevent data
      // corruption due to concurrent write.
      local_filesystem->MoveFile(/*source=*/local_temp_file,
                                 /*target=*/local_cache_file);

      // Copy to destination buffer.
      cur_data_chunk.CopyBufferToRequestedMemory();
    });
  }
  for (auto &cur_thd : threads) {
    D_ASSERT(cur_thd.joinable());
    cur_thd.join();
  }
}

int64_t DiskCacheFileSystem::ReadImpl(FileHandle &handle, void *buffer,
                                      int64_t nr_bytes, idx_t location,
                                      uint64_t block_size) {
  const auto file_size = handle.GetFileSize();

  // No more bytes to read.
  if (location == file_size) {
    return 0;
  }

  const int64_t bytes_to_read =
      std::min<int64_t>(nr_bytes, file_size - location);
  const auto [aligned_start_position, aligned_bytes_to_read] =
      GetStartOffsetAndBytesToRead(handle, location, bytes_to_read,
                                   bytes_to_read, block_size);

  ReadAndCache(handle, static_cast<char *>(buffer), location, bytes_to_read,
               aligned_start_position, aligned_bytes_to_read, bytes_to_read,
               block_size);

  return bytes_to_read;
}

} // namespace duckdb

// TODO(hjiang): Use `resize_without_initialization` to save memset.
// Reference:
// https://github.com/abseil/abseil-cpp/blob/master/absl/strings/internal/resize_uninitialized.h

#include "duckdb/common/thread.hpp"
#include "disk_cache_filesystem.hpp"
#include "crypto.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"

#include <utility>
#include <cstdint>

namespace duckdb {

// TODO(hjiang): Hard code block size for now, in the future we should allow
// global configuration via `SET GLOBAL`.
static uint64_t BLOCK_SIZE = 10000;
static string ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_cached_http_cache";

// IO operations are performed based on block size, this function returns
// aligned start offset and bytes to read.
static std::pair<uint64_t, int64_t>
GetStartOffsetAndBytesToRead(FileHandle &handle, uint64_t start_offset,
                             int64_t bytes_to_read, uint64_t file_size) {
  const uint64_t aligned_start_offset = start_offset / BLOCK_SIZE * BLOCK_SIZE;
  uint64_t aligned_end_offset =
      ((start_offset + bytes_to_read - 1) / BLOCK_SIZE + 1) * BLOCK_SIZE;
  aligned_end_offset = std::min<uint64_t>(aligned_end_offset, file_size);
  return std::make_pair(aligned_start_offset,
                        aligned_end_offset - aligned_start_offset);
}

// Convert SHA256 value to hex string.
static string Sha256ToHexString(const duckdb::hash_bytes &sha256) {
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
static string GetLocalCacheFile(const string &remote_file,
                                uint64_t start_offset, uint64_t bytes_to_read) {
  duckdb::hash_bytes remote_file_sha256_val;
  duckdb::sha256(remote_file.data(), remote_file.length(),
                 remote_file_sha256_val);
  const string remote_file_sha256_str =
      Sha256ToHexString(remote_file_sha256_val);

  const string fname = StringUtil::GetFileName(remote_file);
  return StringUtil::Format("%s/%s.%s-%llu-%llu", ON_DISK_CACHE_DIRECTORY,
                            remote_file_sha256_str, fname, start_offset,
                            bytes_to_read);
}

DiskCacheFileHandle::DiskCacheFileHandle(
    unique_ptr<FileHandle> internal_file_handle_p, DiskCacheFileSystem &fs)
    : FileHandle(fs, internal_file_handle_p->GetPath(),
                 internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {}

DiskCacheFileSystem::DiskCacheFileSystem(
    unique_ptr<FileSystem> internal_filesystem_p)
    : local_filesystem(FileSystem::CreateLocal()),
      internal_filesystem(std::move(internal_filesystem_p)) {
  local_filesystem->CreateDirectory(ON_DISK_CACHE_DIRECTORY);
}

void DiskCacheFileSystem::ReadAndCache(FileHandle &handle, char *start_addr,
                                       uint64_t start_offset,
                                       uint64_t bytes_to_read) {
  D_ASSERT(bytes_to_read >= 1);
  const uint64_t io_op_count = (bytes_to_read - 1) / BLOCK_SIZE + 1;
  vector<thread> threads;
  threads.reserve(io_op_count);
  for (uint64_t idx = 0; idx < io_op_count; ++idx) {
    const uint64_t cur_start_offset = start_offset + BLOCK_SIZE * idx;
    const int64_t cur_bytes_to_read =
        idx < (io_op_count - 1)
            ? BLOCK_SIZE
            : (bytes_to_read - BLOCK_SIZE * (io_op_count - 1));
    char *cur_start_addr = start_addr + idx * BLOCK_SIZE;
    threads.emplace_back(
        [this, &handle, cur_start_offset, cur_bytes_to_read, cur_start_addr]() {
          // Check local cache first, see if we could do a cached read.
          const auto local_cache_file = GetLocalCacheFile(
              handle.GetPath(), cur_start_offset, cur_bytes_to_read);

          // TODO(hjiang): Add documentation and implementation for stale cache
          // eviction policy, before that it's safe to access cache file
          // directly.
          if (local_filesystem->FileExists(local_cache_file)) {
            auto file_handle = local_filesystem->OpenFile(
                local_cache_file, FileOpenFlags::FILE_FLAGS_READ);
            local_filesystem->Read(*file_handle, cur_start_addr,
                                   cur_bytes_to_read, /*location=*/0);
            return;
          }

          // We suffer a cache loss, fallback to remote access then local
          // filesystem write.
          auto &disk_cache_handle = handle.Cast<DiskCacheFileHandle>();
          internal_filesystem->Read(*disk_cache_handle.internal_file_handle,
                                    cur_start_addr, cur_bytes_to_read,
                                    cur_start_offset);

          // TODO(hjiang): Before local cache we should check whether there's
          // enough space left, and trigger a stale file cleanup if necessary.
          //
          // Dump to a temporary location at local filesystem.
          const auto fname = StringUtil::GetFileName(handle.GetPath());
          const auto local_temp_file =
              StringUtil::Format("%s%s.%s", ON_DISK_CACHE_DIRECTORY, fname,
                                 UUID::ToString(UUID::GenerateRandomUUID()));
          {
            auto file_handle = local_filesystem->OpenFile(
                local_temp_file, FileOpenFlags::FILE_FLAGS_WRITE |
                                     FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
            local_filesystem->Write(*file_handle, cur_start_addr,
                                    cur_bytes_to_read, /*location=*/0);
            file_handle->Sync();
          }

          // Then atomically move to the target postion to prevent data
          // corruption due to concurrent write.
          local_filesystem->MoveFile(/*source=*/local_temp_file,
                                     /*target=*/local_cache_file);
        });
  }
  for (auto &cur_thd : threads) {
    D_ASSERT(cur_thd.joinable());
    cur_thd.join();
  }
}

int64_t DiskCacheFileSystem::ReadImpl(FileHandle &handle, void *buffer,
                                      int64_t nr_bytes, idx_t location) {
  const auto file_size = handle.GetFileSize();
  const auto [start_offset, bytes_to_read] =
      GetStartOffsetAndBytesToRead(handle, location, nr_bytes, file_size);

  // TODO(hjiang): The only reason we need `content` is we could have unaligned
  // memory blocks, no need to read into `content` buffer if a read request is
  // perfectly aligned.
  string content(bytes_to_read, '\0');
  ReadAndCache(handle, const_cast<char *>(content.data()), start_offset,
               bytes_to_read);

  const uint64_t actual_bytes_requested =
      std::min<uint64_t>(file_size - location, nr_bytes);
  std::memmove(buffer, const_cast<char *>(content.data()),
               actual_bytes_requested);

  return actual_bytes_requested;
}

} // namespace duckdb

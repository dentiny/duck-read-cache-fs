#include "base_cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"

namespace duckdb {

CacheFileSystemHandle::CacheFileSystemHandle(
    unique_ptr<FileHandle> internal_file_handle_p, CacheFileSystem &fs)
    : FileHandle(fs, internal_file_handle_p->GetPath(),
                 internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {}

void CacheFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                           idx_t location) {
  ReadImpl(handle, buffer, nr_bytes, location, DEFAULT_BLOCK_SIZE);
}
int64_t CacheFileSystem::Read(FileHandle &handle, void *buffer,
                              int64_t nr_bytes) {
  const int64_t bytes_read = ReadImpl(
      handle, buffer, nr_bytes, handle.SeekPosition(), DEFAULT_BLOCK_SIZE);
  handle.Seek(handle.SeekPosition() + bytes_read);
  return bytes_read;
}
int64_t CacheFileSystem::ReadForTesting(FileHandle &handle, void *buffer,
                                        int64_t nr_bytes, idx_t location,
                                        uint64_t block_size) {
  return ReadImpl(handle, buffer, nr_bytes, location, block_size);
}
int64_t CacheFileSystem::ReadImpl(FileHandle &handle, void *buffer,
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

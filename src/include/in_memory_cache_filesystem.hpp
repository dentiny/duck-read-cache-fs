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

namespace duckdb {

// Forward declaration.
class InMemoryCacheFileSystem;

class InMemoryCacheFileHandle : public FileHandle {
public:
  InMemoryCacheFileHandle(unique_ptr<FileHandle> internal_file_handle_p,
                          InMemoryCacheFileSystem &fs);
  ~InMemoryCacheFileHandle() override = default;
  void Close() override {}

private:
  friend class InMemoryCacheFileSystem;
  unique_ptr<FileHandle> internal_file_handle;
};

class InMemoryCacheFileSystem : public FileSystem {
public:
  explicit InMemoryCacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p)
      : InMemoryCacheFileSystem(std::move(internal_filesystem_p),
                                InMemoryCacheConfig{}) {}
  InMemoryCacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p,
                          InMemoryCacheConfig cache_config);
  std::string GetName() const override { return "in_mem_cache_filesystem"; }

  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

  // Expose `Read` interface with testing property injected, underlying it uses
  // the same implementation as production one.
  int64_t ReadForTesting(FileHandle &handle, void *buffer, int64_t nr_bytes,
                         idx_t location, uint64_t block_size);

  // For other API calls, delegate to [internal_filesystem] to handle.
  unique_ptr<FileHandle>
  OpenFile(const string &path, FileOpenFlags flags,
           optional_ptr<FileOpener> opener = nullptr) override {
    auto file_handle = internal_filesystem->OpenFile(
        path, flags | FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS, opener);
    return make_uniq<InMemoryCacheFileHandle>(std::move(file_handle), *this);
  }
  unique_ptr<FileHandle> OpenCompressedFile(unique_ptr<FileHandle> handle,
                                            bool write) override {
    auto file_handle =
        internal_filesystem->OpenCompressedFile(std::move(handle), write);
    return make_uniq<InMemoryCacheFileHandle>(std::move(file_handle), *this);
  }
  void Write(FileHandle &handle, void *buffer, int64_t nr_bytes,
             idx_t location) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    internal_filesystem->Write(*disk_cache_handle.internal_file_handle, buffer,
                               nr_bytes, location);
  }
  int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    return internal_filesystem->Write(*disk_cache_handle.internal_file_handle,
                                      buffer, nr_bytes);
  }
  bool Trim(FileHandle &handle, idx_t offset_bytes,
            idx_t length_bytes) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    return internal_filesystem->Trim(*disk_cache_handle.internal_file_handle,
                                     offset_bytes, length_bytes);
  }
  int64_t GetFileSize(FileHandle &handle) {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    return internal_filesystem->GetFileSize(
        *disk_cache_handle.internal_file_handle);
  }
  time_t GetLastModifiedTime(FileHandle &handle) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    return internal_filesystem->GetLastModifiedTime(
        *disk_cache_handle.internal_file_handle);
  }
  FileType GetFileType(FileHandle &handle) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    return internal_filesystem->GetFileType(
        *disk_cache_handle.internal_file_handle);
  }
  void Truncate(FileHandle &handle, int64_t new_size) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    return internal_filesystem->Truncate(
        *disk_cache_handle.internal_file_handle, new_size);
  }
  bool DirectoryExists(const string &directory,
                       optional_ptr<FileOpener> opener = nullptr) override {
    return internal_filesystem->DirectoryExists(directory, opener);
  }
  void CreateDirectory(const string &directory,
                       optional_ptr<FileOpener> opener = nullptr) override {
    internal_filesystem->CreateDirectory(directory, opener);
  }
  void RemoveDirectory(const string &directory,
                       optional_ptr<FileOpener> opener = nullptr) override {
    internal_filesystem->RemoveDirectory(directory, opener);
  }
  bool ListFiles(const string &directory,
                 const std::function<void(const string &, bool)> &callback,
                 FileOpener *opener = nullptr) override {
    return internal_filesystem->ListFiles(directory, callback, opener);
  }
  void MoveFile(const string &source, const string &target,
                optional_ptr<FileOpener> opener = nullptr) override {
    internal_filesystem->MoveFile(source, target, opener);
  }
  bool FileExists(const string &filename,
                  optional_ptr<FileOpener> opener = nullptr) override {
    return internal_filesystem->FileExists(filename, opener);
  }
  bool IsPipe(const string &filename,
              optional_ptr<FileOpener> opener = nullptr) override {
    return internal_filesystem->IsPipe(filename, opener);
  }
  void RemoveFile(const string &filename,
                  optional_ptr<FileOpener> opener = nullptr) override {
    internal_filesystem->RemoveFile(filename, opener);
  }
  void FileSync(FileHandle &handle) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    internal_filesystem->FileSync(*disk_cache_handle.internal_file_handle);
  }
  string GetHomeDirectory() override {
    return internal_filesystem->GetHomeDirectory();
  }
  string ExpandPath(const string &path) override {
    return internal_filesystem->ExpandPath(path);
  }
  string PathSeparator(const string &path) override {
    return internal_filesystem->PathSeparator(path);
  }
  vector<string> Glob(const string &path,
                      FileOpener *opener = nullptr) override {
    return internal_filesystem->Glob(path, opener);
  }
  void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override {
    internal_filesystem->RegisterSubSystem(std::move(sub_fs));
  }
  void RegisterSubSystem(FileCompressionType compression_type,
                         unique_ptr<FileSystem> fs) override {
    internal_filesystem->RegisterSubSystem(compression_type, std::move(fs));
  }
  void UnregisterSubSystem(const string &name) override {
    internal_filesystem->UnregisterSubSystem(name);
  }
  vector<string> ListSubSystems() override {
    return internal_filesystem->ListSubSystems();
  }
  bool CanHandleFile(const string &fpath) override {
    return internal_filesystem->CanHandleFile(fpath);
  }
  void Seek(FileHandle &handle, idx_t location) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    internal_filesystem->Seek(*disk_cache_handle.internal_file_handle,
                              location);
  }
  void Reset(FileHandle &handle) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    internal_filesystem->Reset(*disk_cache_handle.internal_file_handle);
  }
  idx_t SeekPosition(FileHandle &handle) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    return internal_filesystem->SeekPosition(
        *disk_cache_handle.internal_file_handle);
  }
  // Mutual set acts partially as priority system, which means if multiple
  // filesystem instance could handle a certain path, if mutual set true,
  // first-fit fs instance will be selected. Set cached filesystem always
  // mutually set, so it has higher priority than non-cached version.
  bool IsManuallySet() override { return true; }
  bool CanSeek() override { return internal_filesystem->CanSeek(); }
  bool OnDiskFile(FileHandle &handle) override {
    auto &disk_cache_handle = handle.Cast<InMemoryCacheFileHandle>();
    return internal_filesystem->OnDiskFile(
        *disk_cache_handle.internal_file_handle);
  }
  void SetDisabledFileSystems(const vector<string> &names) override {
    internal_filesystem->SetDisabledFileSystems(names);
  }

private:
  // Read from [handle] for an block-size aligned chunk into [start_addr]; cache
  // to local filesystem and return to user.
  void ReadAndCache(FileHandle &handle, char *buffer,
                    uint64_t requested_start_offset,
                    uint64_t requested_bytes_to_read, uint64_t file_size,
                    uint64_t block_size);

  // Read from [location] on [nr_bytes] for the given [handle] into [buffer].
  // Return the actual number of bytes to read.
  int64_t ReadImpl(FileHandle &handle, void *buffer, int64_t nr_bytes,
                   idx_t location, uint64_t block_size);

  // Read-cache filesystem configuration.
  InMemoryCacheConfig cache_config;
  // Used to access remote files.
  unique_ptr<FileSystem> internal_filesystem;
  // LRU cache to store blocks.
  // TODO(hjiang): Temporarily use string as key, should switch to customized
  // hash.
  ThreadSafeSharedLruCache<string, string> cache;
};

} // namespace duckdb

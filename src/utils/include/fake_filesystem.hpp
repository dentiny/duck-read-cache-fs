// A fake filesystem for cache httpfs extension testing purpose.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

// Forward declaration.
class CacheHttpfsFakeFileSystem;

class CacheHttpfsFakeFsHandle : public FileHandle {
public:
	CacheHttpfsFakeFsHandle(string path, unique_ptr<FileHandle> internal_file_handle_p, CacheHttpfsFakeFileSystem &fs);
	~CacheHttpfsFakeFsHandle() override = default;
	void Close() override {
		internal_file_handle->Close();
	}

	unique_ptr<FileHandle> internal_file_handle;
};

// WARNING: fake filesystem is used for testing purpose and shouldn't be used in production.
class CacheHttpfsFakeFileSystem : public LocalFileSystem {
public:
	CacheHttpfsFakeFileSystem();
	bool CanHandleFile(const string &path) override;
	std::string GetName() const override {
		return "cache_httpfs_fake_filesystem";
	}

	// Delegate to local filesystem.
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	void FileSync(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	bool OnDiskFile(FileHandle &handle) override;

private:
	unique_ptr<FileSystem> local_filesystem;
};

} // namespace duckdb

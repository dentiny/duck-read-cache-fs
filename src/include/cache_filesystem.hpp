// Base class for cache filesystem, including in-memory cache and on-disk cache.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "base_cache_reader.hpp"
#include "base_profile_collector.hpp"
#include "lru_cache.hpp"

namespace duckdb {

// Forward declaration.
class CacheFileSystem;

// File handle used for cache filesystem.
class CacheFileSystemHandle : public FileHandle {
public:
	CacheFileSystemHandle(unique_ptr<FileHandle> internal_file_handle_p, CacheFileSystem &fs);
	~CacheFileSystemHandle() override = default;
	void Close() override {
	}

	unique_ptr<FileHandle> internal_file_handle;
};

class CacheFileSystem : public FileSystem {
public:
	explicit CacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p)
	    : internal_filesystem(std::move(internal_filesystem_p)) {
	}
	~CacheFileSystem() override = default;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr);
	std::string GetName() const override;
	BaseProfileCollector *GetProfileCollector() const {
		return profile_collector.get();
	}
	// Expose for testing purpose.
	BaseCacheReader *GetCacheReader() const {
		return internal_cache_reader;
	}
	// Clear cached content for the given filename (whether it's in-memory or on-disk).
	void ClearCache(const string &fname);
	// Clear all cached content (whether it's in-memory or on-disk).
	void ClearCache();
	// Get file size, which gets cached in-memory.
	int64_t GetFileSize(FileHandle &handle);

	// For other API calls, delegate to [internal_filesystem] to handle.
	unique_ptr<FileHandle> OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) override {
		auto file_handle = internal_filesystem->OpenCompressedFile(std::move(handle), write);
		return make_uniq<CacheFileSystemHandle>(std::move(file_handle), *this);
	}
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		internal_filesystem->Write(*disk_cache_handle.internal_file_handle, buffer, nr_bytes, location);
	}
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		return internal_filesystem->Write(*disk_cache_handle.internal_file_handle, buffer, nr_bytes);
	}
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		return internal_filesystem->Trim(*disk_cache_handle.internal_file_handle, offset_bytes, length_bytes);
	}
	time_t GetLastModifiedTime(FileHandle &handle) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		return internal_filesystem->GetLastModifiedTime(*disk_cache_handle.internal_file_handle);
	}
	FileType GetFileType(FileHandle &handle) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		return internal_filesystem->GetFileType(*disk_cache_handle.internal_file_handle);
	}
	void Truncate(FileHandle &handle, int64_t new_size) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		return internal_filesystem->Truncate(*disk_cache_handle.internal_file_handle, new_size);
	}
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return internal_filesystem->DirectoryExists(directory, opener);
	}
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		internal_filesystem->CreateDirectory(directory, opener);
	}
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		internal_filesystem->RemoveDirectory(directory, opener);
	}
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		return internal_filesystem->ListFiles(directory, callback, opener);
	}
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override {
		internal_filesystem->MoveFile(source, target, opener);
	}
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return internal_filesystem->FileExists(filename, opener);
	}
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return internal_filesystem->IsPipe(filename, opener);
	}
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		internal_filesystem->RemoveFile(filename, opener);
	}
	void FileSync(FileHandle &handle) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
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
	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override {
		return internal_filesystem->Glob(path, opener);
	}
	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override {
		internal_filesystem->RegisterSubSystem(std::move(sub_fs));
	}
	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override {
		internal_filesystem->RegisterSubSystem(compression_type, std::move(fs));
	}
	void UnregisterSubSystem(const string &name) override {
		internal_filesystem->UnregisterSubSystem(name);
	}
	vector<string> ListSubSystems() override {
		return internal_filesystem->ListSubSystems();
	}
	bool CanHandleFile(const string &fpath) override;
	void Seek(FileHandle &handle, idx_t location) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		internal_filesystem->Seek(*disk_cache_handle.internal_file_handle, location);
	}
	void Reset(FileHandle &handle) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		internal_filesystem->Reset(*disk_cache_handle.internal_file_handle);
	}
	idx_t SeekPosition(FileHandle &handle) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		return internal_filesystem->SeekPosition(*disk_cache_handle.internal_file_handle);
	}
	bool IsManuallySet() override;
	bool CanSeek() override {
		return internal_filesystem->CanSeek();
	}
	bool OnDiskFile(FileHandle &handle) override {
		auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
		return internal_filesystem->OnDiskFile(*disk_cache_handle.internal_file_handle);
	}
	void SetDisabledFileSystems(const vector<string> &names) override {
		internal_filesystem->SetDisabledFileSystems(names);
	}

private:
	struct FileMetadata {
		int64_t file_size = 0;
	};

	// Read from [location] on [nr_bytes] for the given [handle] into [buffer].
	// Return the actual number of bytes to read.
	int64_t ReadImpl(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);

	// Initialize cache reader data member, and set to [internal_cache_reader].
	void SetAndGetCacheReader();

	// Initialize profile collector data member.
	void SetProfileCollector();

	// Initialize metadata cache.
	void SetMetadataCache();

	// Used to access remote files.
	unique_ptr<FileSystem> internal_filesystem;
	// Noop, in-memory and on-disk cache reader.
	unique_ptr<BaseCacheReader> noop_cache_reader;
	unique_ptr<BaseCacheReader> in_mem_cache_reader;
	unique_ptr<BaseCacheReader> on_disk_cache_reader;
	// Either in-memory or on-disk cache reader, whichever is actively being used, ownership lies the above cache
	// reader.
	BaseCacheReader *internal_cache_reader = nullptr;
	// Used to profile operations.
	unique_ptr<BaseProfileCollector> profile_collector;
	// Max number of cache entries for file metadata cache.
	inline static constexpr size_t MAX_METADATA_ENTRY = 125;
	using MetadataCache = ThreadSafeSharedLruConstCache<string, FileMetadata>;
	// Metadata cache, which maps from file name to metadata.
	unique_ptr<MetadataCache> metadata_cache;
};

} // namespace duckdb

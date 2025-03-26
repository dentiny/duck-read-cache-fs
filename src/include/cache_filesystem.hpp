// Base class for cache filesystem, including in-memory cache and on-disk cache.

#pragma once

#include "base_cache_reader.hpp"
#include "base_profile_collector.hpp"
#include "cache_reader_manager.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "exclusive_multi_lru_cache.hpp"
#include "shared_lru_cache.hpp"

#include <mutex>
#include <tuple>

namespace duckdb {

// Forward declaration.
class CacheFileSystem;

// File handle used for cache filesystem.
//
// On resource destruction functions (dtor and `Close`), we take different routes on read and write handles.
// A brief summary on how file handle cache is implemented: the key is a wrapper around open flag and path; the value is
// the internal file handle.
// - On cache file handle destruction, if it's read handle, we reset the handle for later reuse and place the internal
// handle into cache; otherwise we do nothing.
// - On cache file handle close operation, if it's write handle, we delegate the call to internal handle; we ignore
// close operation for read handles, because they could be reused, and close usually indicates resource release (i.e.
// close a file descriptor).
//
// For the above design, we suffer resource leak because internal handles are never closed.
// The way cache file handle resolves this problem is:
// - When a value is kicked out of exclusive LRU cache due to eviction policy, it's returned to cache file system, so we
// could close it out of critical section.
// - Before the file handle cache is destructed (i.e. it could be user requesting to disable file handle cache), we get
// all values inside of the cache and close them one by one.

class CacheFileSystemHandle : public FileHandle {
public:
	CacheFileSystemHandle(unique_ptr<FileHandle> internal_file_handle_p, CacheFileSystem &fs);

	// On cache file handle destruction (for read handles), we place internal file handle to file handle cache to later
	// reuse.
	~CacheFileSystemHandle() override;

	// On close, internal file handle could release resource (i.e. close socket file descriptor), which interrupts with
	// file handle cache. So for read handle, we simply do nothing.
	void Close() override;

	// Get internal filesystem for cache filesystem.
	FileSystem *GetInternalFileSystem() const;

	unique_ptr<FileHandle> internal_file_handle;
};

class CacheFileSystem : public FileSystem {
public:
	explicit CacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p)
	    : internal_filesystem(std::move(internal_filesystem_p)), cache_reader_manager(CacheReaderManager::Get()) {
	}
	~CacheFileSystem() override {
		ClearFileHandleCache();
	}

	// Doesn't update file offset (which acts as `PRead` semantics).
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	// Does update file offset (which acts as `Read` semantics).
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr);
	std::string GetName() const override;
	BaseProfileCollector *GetProfileCollector() const {
		return profile_collector.get();
	}
	// Get file size, which gets cached in-memory.
	int64_t GetFileSize(FileHandle &handle);
	// Get cache reader manager.
	shared_ptr<CacheReaderManager> GetCacheReaderManager();
	// Get the internal filesystem for cache filesystem.
	FileSystem *GetInternalFileSystem() const {
		return internal_filesystem.get();
	}

	// Clear all cache inside of cache filesystem (i.e. glob cache, file handle cache, metadata cache).
	// It's worth noting data block cache won't get deleted.
	void ClearCache();

	// Clear cache entries inside of cache filesystem (i.e. glob cache, file handle cache, metadata cache).
	// It's worth noting data block cache won't get deleted.
	void ClearCache(const std::string &filepath);

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
	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;
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
	friend class CacheFileSystemHandle;

	struct FileMetadata {
		int64_t file_size = 0;
	};

	struct FileHandleCacheKey {
		string path;
		FileOpenFlags flags; // flags have parallel access enabled.
		friend std::ostream &operator<<(std::ostream &os, const FileHandleCacheKey &key) {
			os << "path: " << key.path << ", open flags: " << key.flags.GetFlagsInternal();
			return os;
		}
	};
	struct FileHandleCacheKeyEqual {
		bool operator()(const FileHandleCacheKey &lhs, const FileHandleCacheKey &rhs) const {
			const idx_t lhs_flag_value = lhs.flags.GetFlagsInternal();
			const idx_t rhs_flag_value = rhs.flags.GetFlagsInternal();
			return std::tie(lhs.path, lhs_flag_value) == std::tie(rhs.path, rhs_flag_value);
		}
	};
	struct FileHandleCacheKeyHash {
		std::size_t operator()(const FileHandleCacheKey &key) const {
			return std::hash<std::string> {}(key.path) ^ std::hash<idx_t> {}(key.flags.GetFlagsInternal());
		}
	};

	// Initialize global configurations and global objects (i.e. metadata cache, profiler, etc) in a thread-safe manner.
	void InitializeGlobalConfig(optional_ptr<FileOpener> opener);

	// Read from [location] on [nr_bytes] for the given [handle] into [buffer].
	// Return the actual number of bytes to read.
	// It's worth noting file offset won't be updated.
	int64_t ReadImpl(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);

	// Internal implementation for glob operation.
	vector<string> GlobImpl(const string &path, FileOpener *opener);

	// Initialize cache reader data member, and set to [internal_cache_reader].
	void SetAndGetCacheReader();

	// Initialize profile collector data member.
	void SetProfileCollector();

	// Initialize metadata cache.
	void SetMetadataCache();

	// Initialize file handle cache.
	void SetFileHandleCache();

	// Initialize glob cache.
	void SetGlobCache();

	// Clear file handle cache and close all file handle resource inside.
	void ClearFileHandleCache();

	// Clear file handle cache by filepath, and close deleted file handle resource inside.
	void ClearFileHandleCache(const std::string &filepath);

	// Get file handle from cache, or open if it doesn't exist.
	// Return cached file handle.
	unique_ptr<FileHandle> GetOrCreateFileHandleForRead(const string &path, FileOpenFlags flags,
	                                                    optional_ptr<FileOpener> opener);

	// Mutex to protect concurrent access.
	std::mutex cache_reader_mutex;
	// Used to access remote files.
	unique_ptr<FileSystem> internal_filesystem;
	// A global cache reader manager.
	CacheReaderManager &cache_reader_manager;
	// Used to profile operations.
	unique_ptr<BaseProfileCollector> profile_collector;
	// Metadata cache, which maps from file name to metadata.
	using MetadataCache = ThreadSafeSharedLruConstCache<string, FileMetadata>;
	unique_ptr<MetadataCache> metadata_cache;
	// File handle cache, which maps from file name to uncached file handle.
	// Cache is used here to avoid HEAD HTTP request on read operations.
	using FileHandleCache = ThreadSafeExclusiveMultiLruCache<FileHandleCacheKey, FileHandle, FileHandleCacheKeyHash,
	                                                         FileHandleCacheKeyEqual>;
	unique_ptr<FileHandleCache> file_handle_cache;
	// Glob cache, which maps from path to filenames.
	using GlobCache = ThreadSafeSharedLruConstCache<string, vector<string>>;
	unique_ptr<GlobCache> glob_cache;
};

} // namespace duckdb

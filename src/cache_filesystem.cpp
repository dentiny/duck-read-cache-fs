#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "in_memory_cache_reader.hpp"
#include "noop_cache_reader.hpp"
#include "temp_profile_collector.hpp"

namespace duckdb {

CacheFileSystemHandle::CacheFileSystemHandle(unique_ptr<FileHandle> internal_file_handle_p, CacheFileSystem &fs)
    : FileHandle(fs, internal_file_handle_p->GetPath(), internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {
}

FileSystem *CacheFileSystemHandle::GetInternalFileSystem() const {
	auto &cache_filesystem = file_system.Cast<CacheFileSystem>();
	return cache_filesystem.GetInternalFileSystem();
}

void CacheFileSystem::SetMetadataCache() {
	if (!g_enable_metadata_cache) {
		metadata_cache = nullptr;
		return;
	}
	if (metadata_cache == nullptr) {
		metadata_cache = make_uniq<MetadataCache>(MAX_METADATA_ENTRY);
	}
}

void CacheFileSystem::SetProfileCollector() {
	if (g_profile_type == NOOP_PROFILE_TYPE) {
		if (profile_collector == nullptr || profile_collector->GetProfilerType() != NOOP_PROFILE_TYPE) {
			profile_collector = make_uniq<NoopProfileCollector>();
		}
		return;
	}
	if (g_profile_type == TEMP_PROFILE_TYPE) {
		if (profile_collector == nullptr || profile_collector->GetProfilerType() != TEMP_PROFILE_TYPE) {
			profile_collector = make_uniq<TempProfileCollector>();
		}
		return;
	}
	D_ASSERT(false); // Unreachable;
}

bool CacheFileSystem::CanHandleFile(const string &fpath) {
	if (internal_filesystem->CanHandleFile(fpath)) {
		return true;
	}

	// Special handle cases where local filesystem is the internal filesystem.
	//
	// duckdb's implementation is `LocalFileSystem::CanHandle` always returns false, to enable cached local filesystem
	// (i.e. make in-memory cache for disk access), we inherit the virtual filesystem's assumption that local filesystem
	// is the fallback type, which could potentially handles everything.
	//
	// If it doesn't work with local filesystem, an error is returned anyway.
	if (internal_filesystem->GetName() == "LocalFileSystem") {
		return true;
	}

	return false;
}

bool CacheFileSystem::IsManuallySet() {
	// As documented at [CanHandleFile], local filesystem serves as the fallback filesystem for all given paths, return
	// false in certain case so virtual filesystem could pick the most suitable one if exists.
	if (internal_filesystem->GetName() == "LocalFileSystem") {
		return false;
	}

	return true;
}

std::string CacheFileSystem::GetName() const {
	return StringUtil::Format("cache_httpfs with %s", internal_filesystem->GetName());
}

unique_ptr<FileHandle> CacheFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                 optional_ptr<FileOpener> opener) {
	// Initialize cache reader with mutex guard against concurrent access.
	// For duckdb, read operation happens after successful file open, at which point we won't have new configs and read
	// operation happening concurrently.
	{
		std::lock_guard<std::mutex> cache_reader_lck(cache_reader_mutex);
		SetGlobalConfig(opener);
		SetProfileCollector();
		cache_reader_manager.SetCacheReader();
		SetMetadataCache();
		D_ASSERT(profile_collector != nullptr);
		cache_reader_manager.GetCacheReader()->SetProfileCollector(profile_collector.get());
	}

	auto file_handle = internal_filesystem->OpenFile(path, flags | FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS, opener);
	return make_uniq<CacheFileSystemHandle>(std::move(file_handle), *this);
}

void CacheFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ReadImpl(handle, buffer, nr_bytes, location);
}
int64_t CacheFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	const int64_t bytes_read = ReadImpl(handle, buffer, nr_bytes, handle.SeekPosition());
	return bytes_read;
}

int64_t CacheFileSystem::GetFileSize(FileHandle &handle) {
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();

	// Stat without cache involved.
	if (metadata_cache == nullptr) {
		return internal_filesystem->GetFileSize(*disk_cache_handle.internal_file_handle);
	}

	// Stat with cache.
	bool metadata_cache_hit = true;
	auto metadata =
	    metadata_cache->GetOrCreate(disk_cache_handle.internal_file_handle->GetPath(),
	                                [this, &disk_cache_handle, &metadata_cache_hit](const string & /*unused*/) {
		                                metadata_cache_hit = false;
		                                const int64_t file_size =
		                                    internal_filesystem->GetFileSize(*disk_cache_handle.internal_file_handle);
		                                auto file_metadata = make_shared_ptr<FileMetadata>();
		                                file_metadata->file_size = file_size;
		                                return file_metadata;
	                                });
	const BaseProfileCollector::CacheAccess cache_access = metadata_cache_hit
	                                                           ? BaseProfileCollector::CacheAccess::kCacheHit
	                                                           : BaseProfileCollector::CacheAccess::kCacheMiss;
	GetProfileCollector()->RecordCacheAccess(BaseProfileCollector::CacheEntity::kMetadata, cache_access);
	return metadata->file_size;
}
int64_t CacheFileSystem::ReadImpl(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	const auto file_size = GetFileSize(handle);

	// No more bytes to read.
	if (location >= static_cast<idx_t>(file_size)) {
		return 0;
	}

	const int64_t bytes_to_read = MinValue<int64_t>(nr_bytes, file_size - location);
	cache_reader_manager.GetCacheReader()->ReadAndCache(handle, static_cast<char *>(buffer), location, bytes_to_read,
	                                                    file_size);

// Check actually read content with bytes read from internal filesystem. Only enabled in DEBUG build.
#if defined(DEBUG)
	string check_buffer(bytes_to_read, '\0');
	auto &cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto *internal_filesystem = cache_handle.GetInternalFileSystem();
	internal_filesystem->Read(*cache_handle.internal_file_handle, const_cast<char *>(check_buffer.data()),
	                          bytes_to_read, location);
	D_ASSERT(check_buffer == string(const_cast<char *>(check_buffer.data()), bytes_to_read));
#endif

	// TODO(hjiang): there're some unresolved problems with duckdb's filesystem, for example, it's not clear on `Read`
	// and `PRead`'s behavior. Related issue:
	// - https://github.com/duckdb/duckdb/issues/16362
	// - https://github.com/duckdb/duckdb-httpfs/issues/16
	//
	// Here we adhere to httpfs's behavior.
	Seek(handle, location + bytes_to_read);

	return bytes_to_read;
}

} // namespace duckdb

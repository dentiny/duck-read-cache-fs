#include "cache_filesystem.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "in_memory_cache_reader.hpp"
#include "noop_cache_reader.hpp"
#include "temp_profile_collector.hpp"
#include "utils/include/time_utils.hpp"

namespace duckdb {

namespace {
// For certain filesystems, file open info contains "file_size" field in the extended stats map.
constexpr const char* FILE_SIZE_INFO_KEY = "file_size";
// For certain filesystems, file open info contains "last_modified" field in the extended stats map.
constexpr const char* LAST_MOD_TIMESTAMP_KEY = "last_modified";
}  // namespace

CacheFileSystemHandle::CacheFileSystemHandle(unique_ptr<FileHandle> internal_file_handle_p, CacheFileSystem& fs, std::function<void(CacheFileSystemHandle&)> dtor_callback_p)
    : FileHandle(fs, internal_file_handle_p->GetPath(), internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)),
	  dtor_callback(std::move(dtor_callback_p)) {
}

FileSystem *CacheFileSystemHandle::GetInternalFileSystem() const {
	auto &cache_filesystem = file_system.Cast<CacheFileSystem>();
	return cache_filesystem.GetInternalFileSystem();
}

CacheFileSystemHandle::~CacheFileSystemHandle() {
	if (dtor_callback) {
		dtor_callback(*this);
	}
}

void CacheFileSystemHandle::Close() {
	if (!flags.OpenForReading()) {
		internal_file_handle->Close();
	}
}

void CacheFileSystem::SetMetadataCache() {
	if (!g_enable_metadata_cache) {
		metadata_cache = nullptr;
		return;
	}
	if (metadata_cache == nullptr) {
		metadata_cache = make_uniq<MetadataCache>(g_max_metadata_cache_entry, g_metadata_cache_entry_timeout_millisec);
	}
}

void CacheFileSystem::SetGlobCache() {
	if (!g_enable_glob_cache) {
		glob_cache = nullptr;
		return;
	}
	if (glob_cache == nullptr) {
		glob_cache = make_uniq<GlobCache>(g_max_glob_cache_entry, g_glob_cache_entry_timeout_millisec);
	}
}

void CacheFileSystem::ClearFileHandleCache() {
	if (file_handle_cache == nullptr) {
		return;
	}
	auto file_handles = file_handle_cache->ClearAndGetValues();
	for (auto &cur_file_handle : file_handles) {
		cur_file_handle->Close();
	}
	file_handle_cache = nullptr;
	in_use_file_handle_counter = nullptr;
}

void CacheFileSystem::ClearFileHandleCache(const std::string &filepath) {
	if (file_handle_cache == nullptr) {
		return;
	}
	auto file_handles = file_handle_cache->ClearAndGetValues(
	    [&filepath](const FileHandleCacheKey &handle_key) { return handle_key.path == filepath; });
	for (auto &cur_file_handle : file_handles) {
		cur_file_handle->Close();
	}
}

void CacheFileSystem::SetFileHandleCache() {
	if (!g_enable_file_handle_cache) {
		ClearFileHandleCache();
		return;
	}
	if (file_handle_cache == nullptr) {
		file_handle_cache =
			make_shared_ptr<FileHandleCache>(g_max_file_handle_cache_entry, g_file_handle_cache_entry_timeout_millisec);
	}
	if (in_use_file_handle_counter == nullptr) {
		in_use_file_handle_counter = make_shared_ptr<InUseFileHandleCounter>();
	}
}

void CacheFileSystem::SetProfileCollector() {
	if (*g_profile_type == *NOOP_PROFILE_TYPE) {
		if (profile_collector == nullptr || profile_collector->GetProfilerType() != *NOOP_PROFILE_TYPE) {
			profile_collector = make_uniq<NoopProfileCollector>();
		}
		return;
	}
	if (*g_profile_type == *TEMP_PROFILE_TYPE) {
		if (profile_collector == nullptr || profile_collector->GetProfilerType() != *TEMP_PROFILE_TYPE) {
			profile_collector = make_uniq<TempProfileCollector>();
		}
		return;
	}
	D_ASSERT(false); // Unreachable;
}

void CacheFileSystem::ClearCache() {
	if (metadata_cache != nullptr) {
		metadata_cache->Clear();
	}
	if (glob_cache != nullptr) {
		glob_cache->Clear();
	}
	ClearFileHandleCache();
}

void CacheFileSystem::ClearCache(const std::string &filepath) {
	if (metadata_cache != nullptr) {
		metadata_cache->Clear([&filepath](const std::string &key) { return key == filepath; });
	}
	if (glob_cache != nullptr) {
		glob_cache->Clear([&filepath](const std::string &key) { return key == filepath; });
	}
	ClearFileHandleCache(filepath);
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

unique_ptr<FileHandle> CacheFileSystem::CreateCacheFileHandleForRead(unique_ptr<FileHandle> internal_file_handle) {
	const auto flags = internal_file_handle->GetFlags();
	D_ASSERT(flags.OpenForReading());

	CacheFileSystem::FileHandleCacheKey cache_key {
		.path = internal_file_handle->GetPath(),
		.flags = flags | FileFlags::FILE_FLAGS_PARALLEL_ACCESS,
	};
	if (in_use_file_handle_counter != nullptr) {
		in_use_file_handle_counter->Increment(cache_key);
	}

	auto dtor_callback = [cache_key = std::move(cache_key), file_handle_cache = file_handle_cache, in_use_file_handle_counter = in_use_file_handle_counter](CacheFileSystemHandle& file_handle) {
		// Reset file handle state (i.e. file offset) before placing into cache.
		file_handle.internal_file_handle->Reset();
		if (file_handle_cache == nullptr) {
			D_ASSERT(in_use_file_handle_counter == nullptr);
			return;
		}

		// Place internal file handle back to file handle cache after reset.
		auto evicted_handle = file_handle_cache->Put(std::move(cache_key), std::move(file_handle.internal_file_handle));
		if (evicted_handle != nullptr) {
			evicted_handle->Close();
		}
		in_use_file_handle_counter->Decrement(cache_key);
	};
	return make_uniq<CacheFileSystemHandle>(std::move(internal_file_handle), *this, std::move(dtor_callback));
}

shared_ptr<CacheFileSystem::FileMetadata> CacheFileSystem::Stats(FileHandle &handle) {
	// Get file size.
	const int64_t file_size = internal_filesystem->GetFileSize(handle);

	// Get last modification timestamp.
	const time_t last_modification_time = internal_filesystem->GetLastModifiedTime(handle);

	FileMetadata file_metadata {
		.file_size = file_size,
		.last_modification_time = last_modification_time,
	};
	return make_shared_ptr<FileMetadata>(std::move(file_metadata));
}

vector<OpenFileInfo> CacheFileSystem::GlobImpl(const string &path, FileOpener *opener) {
	const auto oper_id = profile_collector->GenerateOperId();
	profile_collector->RecordOperationStart(BaseProfileCollector::IoOperation::kGlob, oper_id);
	auto open_file_info = internal_filesystem->Glob(path, opener);
	profile_collector->RecordOperationEnd(BaseProfileCollector::IoOperation::kGlob, oper_id);

	// Certain filesystem (i.e., s3 filesystem) populates file size within extended file info, make an attempt to extract file size.
	//
	// Initialize metadata cache if not.
	SetMetadataCache();
	if (metadata_cache == nullptr) {
		return open_file_info;
	}

	// Attempt to fill in metadata cache.
	for (const auto& cur_file_info : open_file_info) {
		const auto& filepath = cur_file_info.path;
		const auto& cur_extended_file_info = *cur_file_info.extended_info;

		// Attempt to get file size from extended file info.
		auto iter = cur_extended_file_info.options.find(FILE_SIZE_INFO_KEY);
		if (iter == cur_extended_file_info.options.end()) {
			continue;
		}
		auto& file_size_value = iter->second;
		const int64_t file_size = file_size_value.GetValue<int64_t>();

		// Attempt to get last modification timestamp from extended file info.
		iter = cur_extended_file_info.options.find(LAST_MOD_TIMESTAMP_KEY);
		if (iter == cur_extended_file_info.options.end()) {
			continue;
		}
		auto& last_modification_time_value = iter->second;
		const time_t last_modification_time = DuckdbTimestampToTimeT(last_modification_time_value.GetValue<timestamp_t>());

		FileMetadata file_metadata {
			.file_size = file_size_value.GetValue<int64_t>(),
			.last_modification_time = last_modification_time,
		};
		metadata_cache->Put(filepath, make_shared_ptr<FileMetadata>(std::move(file_metadata)));
	}
	return open_file_info;
}

vector<OpenFileInfo> CacheFileSystem::Glob(const string &path, FileOpener *opener) {
	InitializeGlobalConfig(opener);
	if (glob_cache == nullptr) {
		return GlobImpl(path, opener);
	}

	// If it's a string without glob expression, we neither record IO latency, nor place it into cache, otherwise
	// latency distribution and glob cache will be populated.
	if (!FileSystem::HasGlob(path)) {
		return internal_filesystem->Glob(path, opener);
	}

	bool glob_cache_hit = true;
	auto res = glob_cache->GetOrCreate(path, [this, &path, opener, &glob_cache_hit](const string & /*unused*/) {
		glob_cache_hit = false;
		auto glob_res = GlobImpl(path, opener);
		return make_shared_ptr<vector<OpenFileInfo>>(std::move(glob_res));
	});
	const BaseProfileCollector::CacheAccess cache_access =
	    glob_cache_hit ? BaseProfileCollector::CacheAccess::kCacheHit : BaseProfileCollector::CacheAccess::kCacheMiss;
	GetProfileCollector()->RecordCacheAccess(BaseProfileCollector::CacheEntity::kGlob, cache_access);
	return *res;
}

void CacheFileSystem::InitializeGlobalConfig(optional_ptr<FileOpener> opener) {
	// Initialize cache reader with mutex guard against concurrent access.
	// For duckdb, read operation happens after successful file open, at which point we won't have new configs and read
	// operation happening concurrently.
	std::lock_guard<std::mutex> cache_reader_lck(cache_reader_mutex);
	SetGlobalConfig(opener);
	SetProfileCollector();
	cache_reader_manager.SetCacheReader();
	SetMetadataCache();
	SetFileHandleCache();
	SetGlobCache();
	D_ASSERT(profile_collector != nullptr);
	cache_reader_manager.GetCacheReader()->SetProfileCollector(profile_collector.get());
}

unique_ptr<FileHandle> CacheFileSystem::GetOrCreateFileHandleForRead(const string &path, FileOpenFlags flags,
                                                                     optional_ptr<FileOpener> opener) {
	D_ASSERT(flags.OpenForReading());

	// Cache is exclusive, so we don't need to acquire lock for avoid repeated access.
	if (file_handle_cache != nullptr) {
		FileHandleCacheKey key {
		    .path = path,
		    .flags = flags | FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS,
		};
		auto get_and_pop_res = file_handle_cache->GetAndPop(key);
		for (auto &cur_val : get_and_pop_res.evicted_items) {
			cur_val->Close();
		}
		if (get_and_pop_res.target_item != nullptr) {
			GetProfileCollector()->RecordCacheAccess(BaseProfileCollector::CacheEntity::kFileHandle,
			                                         BaseProfileCollector::CacheAccess::kCacheHit);
			return CreateCacheFileHandleForRead(std::move(get_and_pop_res.target_item));
		}

		// Record stats on cache miss.
		GetProfileCollector()->RecordCacheAccess(BaseProfileCollector::CacheEntity::kFileHandle,
		                                         BaseProfileCollector::CacheAccess::kCacheMiss);
		
	    // Record cache miss caused by exclusive resource in use.
		const unsigned in_use_count = in_use_file_handle_counter->GetCount(key);
		if (in_use_count > 0) {
			GetProfileCollector()->RecordCacheAccess(BaseProfileCollector::CacheEntity::kFileHandle,
				BaseProfileCollector::CacheAccess::kCacheEntryInUse);
		}
	}

	const auto oper_id = profile_collector->GenerateOperId();
	profile_collector->RecordOperationStart(BaseProfileCollector::IoOperation::kOpen, oper_id);
	auto file_handle = internal_filesystem->OpenFile(path, flags | FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS, opener);
	profile_collector->RecordOperationEnd(BaseProfileCollector::IoOperation::kOpen, oper_id);
	return CreateCacheFileHandleForRead(std::move(file_handle));
}

unique_ptr<FileHandle> CacheFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                 optional_ptr<FileOpener> opener) {
	InitializeGlobalConfig(opener);
	if (flags.OpenForReading()) {
		return GetOrCreateFileHandleForRead(path, flags, opener);
	}

	// Otherwise, we do nothing (i.e. profiling) but wrapping it with cache file handle wrapper.
	auto file_handle = internal_filesystem->OpenFile(path, flags, opener);
	return make_uniq<CacheFileSystemHandle>(std::move(file_handle), *this, /*dtor_callback=*/[](CacheFileSystemHandle& /*unused*/){});
}

void CacheFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ReadImpl(handle, buffer, nr_bytes, location);
}
int64_t CacheFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	const idx_t offset = handle.SeekPosition();
	const int64_t bytes_read = ReadImpl(handle, buffer, nr_bytes, offset);
	Seek(handle, offset + bytes_read);
	return bytes_read;
}

time_t CacheFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();

	// Stat without cache involved.
	if (metadata_cache == nullptr) {
		return internal_filesystem->GetLastModifiedTime(*disk_cache_handle.internal_file_handle);
	}

	// Stat with cache.
	bool metadata_cache_hit = true;
	auto metadata =
	    metadata_cache->GetOrCreate(disk_cache_handle.internal_file_handle->GetPath(),
	                                [this, &disk_cache_handle, &metadata_cache_hit](const string & /*unused*/) {
		                                metadata_cache_hit = false;
										return Stats(*disk_cache_handle.internal_file_handle);
	                                });
	const BaseProfileCollector::CacheAccess cache_access = metadata_cache_hit
	                                                           ? BaseProfileCollector::CacheAccess::kCacheHit
	                                                           : BaseProfileCollector::CacheAccess::kCacheMiss;
	GetProfileCollector()->RecordCacheAccess(BaseProfileCollector::CacheEntity::kMetadata, cache_access);
	return metadata->last_modification_time;
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
										return Stats(*disk_cache_handle.internal_file_handle);
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

	return bytes_to_read;
}

} // namespace duckdb

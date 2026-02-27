#include "cache_filesystem.hpp"

#include "assert_utils.hpp"
#include "cache_filesystem_logger.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "in_memory_cache_reader.hpp"
#include "utils/include/url_utils.hpp"

namespace duckdb {

namespace {
// For certain filesystems, file open info contains "file_size" field in the extended stats map.
constexpr const char *FILE_SIZE_INFO_KEY = "file_size";
// For certain filesystems, file open info contains "last_modified" field in the extended stats map.
constexpr const char *LAST_MOD_TIMESTAMP_KEY = "last_modified";

connection_t GetConnectionId(optional_ptr<FileOpener> opener) {
	if (!opener) {
		return 0;
	}
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (!client_context) {
		return 0;
	}
	return client_context->GetConnectionId();
}

connection_t GetConnectionIdFromContext(const QueryContext &context) {
	if (!context.Valid()) {
		return DConstants::INVALID_INDEX;
	}
	auto client_context = context.GetClientContext();
	if (!client_context) {
		return DConstants::INVALID_INDEX;
	}
	return client_context->GetConnectionId();
}

} // namespace

CacheFileSystem::FileHandleCacheKey::FileHandleCacheKey(const string &path_arg, FileOpenFlags flags_arg)
    : path(SanitizedCachePath(path_arg).Path()), flags(flags_arg | FileFlags::FILE_FLAGS_PARALLEL_ACCESS) {
}

CacheFileSystemHandle::CacheFileSystemHandle(unique_ptr<FileHandle> internal_file_handle_p, CacheFileSystem &fs,
                                             std::function<void(CacheFileSystemHandle &)> dtor_callback_p,
                                             connection_t connection_id_p)
    : FileHandle(fs, internal_file_handle_p->GetPath(), internal_file_handle_p->GetFlags()),
      logger(internal_file_handle_p->logger), internal_file_handle(std::move(internal_file_handle_p)),
      dtor_callback(std::move(dtor_callback_p)), connection_id(connection_id_p) {
}

FileSystem *CacheFileSystemHandle::GetInternalFileSystem() const {
	auto &cache_filesystem = file_system.Cast<CacheFileSystem>();
	return cache_filesystem.GetInternalFileSystem();
}

string CacheFileSystemHandle::GetVersionTag() {
	auto *internal_filesystem = GetInternalFileSystem();
	return internal_filesystem->GetVersionTag(*internal_file_handle);
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

CacheFileSystem::CacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p,
                                 weak_ptr<CacheHttpfsInstanceState> instance_state_p)
    : internal_filesystem(std::move(internal_filesystem_p)), instance_state(std::move(instance_state_p)) {
	auto state = instance_state.lock();
	if (!state) {
		throw InternalException("CacheFileSystem: instance state is no longer valid during construction");
	}
	state->registry.Register(this);
}
CacheFileSystem::~CacheFileSystem() {
	// Unregister from per-instance registry before destruction
	auto state = instance_state.lock();
	if (state) {
		state->registry.Unregister(this);
	}
	ClearFileHandleCache();
}

bool CacheFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                FileOpener *opener) {
	auto wrapped_callback = [this, &callback](OpenFileInfo &info) {
		const bool is_dir = this->IsDirectory(info.path);
		callback(info.path, is_dir);
	};
	return ListFilesExtended(directory, wrapped_callback, opener);
}

bool CacheFileSystem::ListFilesExtended(const string &directory,
                                        const std::function<void(OpenFileInfo &info)> &callback,
                                        optional_ptr<FileOpener> opener) {
	return internal_filesystem->ListFiles(directory, callback, opener);
}

void CacheFileSystem::SetMetadataCache() {
	const auto &config = instance_state.lock()->config;
	if (!config.enable_metadata_cache) {
		metadata_cache = nullptr;
		return;
	}
	if (metadata_cache == nullptr) {
		metadata_cache =
		    make_uniq<MetadataCache>(config.max_metadata_cache_entry, config.metadata_cache_entry_timeout_millisec);
	}
}

bool CacheFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto conn_id = GetConnectionId(opener);
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, conn_id);
	const auto latency_guard = collector.RecordOperationStart(IoOperation::kFileRemove);
	ClearCache(filename, conn_id);
	return internal_filesystem->TryRemoveFile(filename, opener);
}

void CacheFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto conn_id = GetConnectionId(opener);
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, conn_id);
	const auto latency_guard = collector.RecordOperationStart(IoOperation::kFileRemove);
	ClearCache(filename, conn_id);
	internal_filesystem->RemoveFile(filename, opener);
}

void CacheFileSystem::SetGlobCache() {
	const auto &config = instance_state.lock()->config;
	if (!config.enable_glob_cache) {
		glob_cache = nullptr;
		return;
	}
	if (glob_cache == nullptr) {
		glob_cache = make_uniq<GlobCache>(config.max_glob_cache_entry, config.glob_cache_entry_timeout_millisec);
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

void CacheFileSystem::ClearFileHandleCache(const string &filepath) {
	if (file_handle_cache == nullptr) {
		return;
	}
	const SanitizedCachePath cache_key {filepath};
	auto file_handles = file_handle_cache->ClearAndGetValues(
	    [&cache_key](const FileHandleCacheKey &handle_key) { return handle_key.path == cache_key.Path(); });
	for (auto &cur_file_handle : file_handles) {
		cur_file_handle->Close();
	}
}

void CacheFileSystem::SetFileHandleCache() {
	const auto &config = instance_state.lock()->config;
	if (!config.enable_file_handle_cache) {
		ClearFileHandleCache();
		return;
	}
	if (file_handle_cache == nullptr) {
		file_handle_cache = make_shared_ptr<FileHandleCache>(config.max_file_handle_cache_entry,
		                                                     config.file_handle_cache_entry_timeout_millisec);
	}
	if (in_use_file_handle_counter == nullptr) {
		in_use_file_handle_counter = make_shared_ptr<InUseFileHandleCounter>();
	}
}

void CacheFileSystem::ClearCache() {
	if (metadata_cache != nullptr) {
		metadata_cache->Clear();
	}
	if (glob_cache != nullptr) {
		glob_cache->Clear();
	}
	ClearFileHandleCache();
	// TODO(hjiang): This seems a duplicate function call, extension statement funtion has already cleared the cache.
	instance_state.lock()->cache_reader_manager.ClearCache();
}

void CacheFileSystem::ClearCache(const string &filepath, connection_t conn_id) {
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, conn_id);
	const auto latency_guard = collector.RecordOperationStart(IoOperation::kFilePathCacheClear);
	const SanitizedCachePath cache_key {filepath};
	if (metadata_cache != nullptr) {
		metadata_cache->Clear([&cache_key](const string &key) { return key == cache_key.Path(); });
	}
	if (glob_cache != nullptr) {
		glob_cache->Clear([&cache_key](const string &key) { return key == cache_key.Path(); });
	}
	ClearFileHandleCache(cache_key);
	// TODO(hjiang): This seems a duplicate function call, extension statement funtion has already cleared the cache.
	instance_state.lock()->cache_reader_manager.ClearCache(cache_key);
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

string CacheFileSystem::GetName() const {
	return StringUtil::Format("cache_httpfs_%s", internal_filesystem->GetName());
}

void CacheFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, disk_cache_handle.GetConnectionId());
	const auto latency_guard = collector.RecordOperationStart(IoOperation::kWrite);
	internal_filesystem->Write(*disk_cache_handle.internal_file_handle, buffer, nr_bytes, location);
	collector.RecordBytesWritten(nr_bytes);
}

int64_t CacheFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, disk_cache_handle.GetConnectionId());
	const auto latency_guard = collector.RecordOperationStart(IoOperation::kWrite);
	auto bytes_written = internal_filesystem->Write(*disk_cache_handle.internal_file_handle, buffer, nr_bytes);
	collector.RecordBytesWritten(bytes_written);
	return bytes_written;
}

unique_ptr<FileHandle> CacheFileSystem::CreateCacheFileHandleForRead(unique_ptr<FileHandle> internal_file_handle,
                                                                     connection_t conn_id) {
	if (internal_file_handle == nullptr) {
		return nullptr;
	}

	const auto flags = internal_file_handle->GetFlags();
	D_ASSERT(flags.OpenForReading());

	CacheFileSystem::FileHandleCacheKey cache_key {internal_file_handle->GetPath(), flags};
	if (in_use_file_handle_counter != nullptr) {
		in_use_file_handle_counter->Increment(cache_key);
	}

	auto dtor_callback = [cache_key = std::move(cache_key), file_handle_cache = file_handle_cache,
	                      in_use_file_handle_counter = in_use_file_handle_counter](CacheFileSystemHandle &file_handle) {
		// Reset file handle state (i.e. file offset) before placing into cache.
		file_handle.internal_file_handle->Reset();
		if (file_handle_cache == nullptr) {
			CACHE_HTTPFS_ALWAYS_ASSERT(in_use_file_handle_counter == nullptr);
			return;
		}

		// Place internal file handle back to file handle cache after reset.
		auto evicted_handle = file_handle_cache->Put(cache_key, std::move(file_handle.internal_file_handle));
		if (evicted_handle != nullptr) {
			evicted_handle->Close();
		}
		in_use_file_handle_counter->Decrement(cache_key);
	};
	return make_uniq<CacheFileSystemHandle>(std::move(internal_file_handle), *this, std::move(dtor_callback), conn_id);
}

shared_ptr<CacheFileSystem::FileMetadata> CacheFileSystem::Stats(FileHandle &handle) {
	// Get file size.
	const int64_t file_size = internal_filesystem->GetFileSize(handle);

	// Get last modification timestamp.
	const timestamp_t last_modification_time = internal_filesystem->GetLastModifiedTime(handle);

	FileMetadata file_metadata;
	file_metadata.file_size = file_size;
	file_metadata.last_modification_time = last_modification_time;
	return make_shared_ptr<FileMetadata>(std::move(file_metadata));
}

vector<OpenFileInfo> CacheFileSystem::GlobImpl(const string &path, FileOpener *opener) {
	auto conn_id = GetConnectionId(opener);
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, conn_id);
	const auto latency_guard = collector.RecordOperationStart(IoOperation::kGlob);
	auto open_file_info = internal_filesystem->Glob(path, opener);

	// Certain filesystem (i.e., s3 filesystem) populates file size within extended file info, make an attempt to
	// extract file size.
	//
	// Initialize metadata cache if not.
	{
		const concurrency::lock_guard<concurrency::mutex> lck(cache_reader_mutex);
		SetMetadataCache();
	}
	if (metadata_cache == nullptr) {
		return open_file_info;
	}

	// Attempt to fill in metadata cache.
	for (const auto &cur_file_info : open_file_info) {
		const auto &filepath = cur_file_info.path;
		if (cur_file_info.extended_info == nullptr) {
			continue;
		}

		// Attempt to get file size from extended file info.
		const auto &cur_extended_file_info = *cur_file_info.extended_info;
		auto iter = cur_extended_file_info.options.find(FILE_SIZE_INFO_KEY);
		if (iter == cur_extended_file_info.options.end()) {
			continue;
		}
		auto &file_size_value = iter->second;
		const int64_t file_size = file_size_value.GetValue<int64_t>();

		// Attempt to get last modification timestamp from extended file info.
		iter = cur_extended_file_info.options.find(LAST_MOD_TIMESTAMP_KEY);
		if (iter == cur_extended_file_info.options.end()) {
			continue;
		}
		auto &last_modification_time_value = iter->second;
		const timestamp_t last_modification_time = last_modification_time_value.GetValue<timestamp_t>();

		FileMetadata file_metadata;
		file_metadata.file_size = file_size;
		file_metadata.last_modification_time = last_modification_time;
		metadata_cache->Put(filepath, make_shared_ptr<FileMetadata>(std::move(file_metadata)));
	}
	return open_file_info;
}

vector<OpenFileInfo> CacheFileSystem::Glob(const string &path, FileOpener *opener) {
	auto conn_id = GetConnectionId(opener);
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
	const CacheAccess cache_access = glob_cache_hit ? CacheAccess::kCacheHit : CacheAccess::kCacheMiss;
	RecordCacheAccess(conn_id, CacheEntity::kGlob, cache_access);
	return *res;
}

// TODO(hjiang): remove the function and switch to extension setting callback.
void CacheFileSystem::InitializeGlobalConfig(optional_ptr<FileOpener> opener) {
	auto instance_state_locked = instance_state.lock();
	const concurrency::lock_guard<concurrency::mutex> cache_reader_lck(cache_reader_mutex);

	auto conn_id = GetConnectionId(opener);
	instance_state_locked->profile_collector_manager.SetProfileCollector(conn_id,
	                                                                     instance_state_locked->config.profile_type);

	if (opener) {
		auto client_context = FileOpener::TryGetClientContext(opener);
		if (client_context) {
			RegisterConnectionCleanupState(*client_context, instance_state);
		}
	}

	instance_state_locked->cache_reader_manager.SetCacheReader(instance_state_locked->config, instance_state);

	SetMetadataCache();
	SetFileHandleCache();
	SetGlobCache();
}

unique_ptr<FileHandle> CacheFileSystem::GetOrCreateFileHandleForRead(const OpenFileInfo &file, FileOpenFlags flags,
                                                                     optional_ptr<FileOpener> opener) {
	D_ASSERT(flags.OpenForReading());
	auto conn_id = GetConnectionId(opener);
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, conn_id);

	// Cache is exclusive, so we don't need to acquire lock for avoid repeated access.
	if (file_handle_cache != nullptr) {
		FileHandleCacheKey key {file.path, flags};
		auto get_and_pop_res = file_handle_cache->GetAndPop(key);
		for (auto &cur_val : get_and_pop_res.evicted_items) {
			cur_val->Close();
		}
		if (get_and_pop_res.target_item != nullptr) {
			collector.RecordCacheAccess(CacheEntity::kFileHandle, CacheAccess::kCacheHit);
			DUCKDB_LOG_OPEN_CACHE_HIT((*get_and_pop_res.target_item));
			return CreateCacheFileHandleForRead(std::move(get_and_pop_res.target_item), conn_id);
		}

		// Record stats on cache miss.
		collector.RecordCacheAccess(CacheEntity::kFileHandle, CacheAccess::kCacheMiss);

		// Record cache miss caused by exclusive resource in use.
		const unsigned in_use_count = in_use_file_handle_counter->GetCount(key);
		if (in_use_count > 0) {
			collector.RecordCacheAccess(CacheEntity::kFileHandle, CacheAccess::kCacheEntryInUse);
		}
	}

	const auto latency_guard = collector.RecordOperationStart(IoOperation::kOpen);
	auto file_handle = internal_filesystem->OpenFile(file, flags | FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS, opener);
	DUCKDB_LOG_OPEN_CACHE_MISS_PTR((file_handle));
	return CreateCacheFileHandleForRead(std::move(file_handle), conn_id);
}

// If the target file is compressed, here we only read and potentially cache compressed content.
// Based on duckdb virtual filesystem's implementation, decompression happens after raw bytes read over.
// Same strategy applies for duckdb internal "external file cache".
unique_ptr<FileHandle> CacheFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                         optional_ptr<FileOpener> opener) {
	InitializeGlobalConfig(opener);

	// If setting has already been specified to clear cache, we clear it only once at file open.
	if (instance_state.lock()->config.clear_cache_on_write) {
		ClearCache(file.path, GetConnectionId(opener));
	}

	if (flags.OpenForReading()) {
		return GetOrCreateFileHandleForRead(file, flags, opener);
	}

	// Otherwise, we do nothing (i.e. profiling) but wrapping it with cache file handle wrapper.
	// For write handles, we still need the connection_id for consistency
	auto conn_id = GetConnectionId(opener);
	auto file_handle = internal_filesystem->OpenFile(file, flags, opener);
	return make_uniq<CacheFileSystemHandle>(
	    std::move(file_handle), *this,
	    /*dtor_callback=*/[](CacheFileSystemHandle & /*unused*/) {}, conn_id);
}

unique_ptr<FileHandle> CacheFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                 optional_ptr<FileOpener> opener) {
	return OpenFileExtended(OpenFileInfo(path), flags, opener);
}

unique_ptr<FileHandle> CacheFileSystem::OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle,
                                                           bool write) {
	auto file_handle = internal_filesystem->OpenCompressedFile(context, std::move(handle), write);
	const connection_t conn_id = GetConnectionIdFromContext(context);
	return make_uniq<CacheFileSystemHandle>(
	    std::move(file_handle), *this,
	    /*dtor_callback=*/[](CacheFileSystemHandle & /*unused*/) {}, conn_id);
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

timestamp_t CacheFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();

	// Stat without cache involved.
	if (metadata_cache == nullptr) {
		return internal_filesystem->GetLastModifiedTime(*disk_cache_handle.internal_file_handle);
	}

	// Stat with cache.
	bool metadata_cache_hit = true;
	const SanitizedCachePath cache_key {disk_cache_handle.internal_file_handle->GetPath()};
	auto metadata = metadata_cache->GetOrCreate(
	    cache_key, [this, &disk_cache_handle, &metadata_cache_hit](const string & /*unused*/) {
		    metadata_cache_hit = false;
		    return Stats(*disk_cache_handle.internal_file_handle);
	    });
	const CacheAccess cache_access = metadata_cache_hit ? CacheAccess::kCacheHit : CacheAccess::kCacheMiss;
	RecordCacheAccess(disk_cache_handle.GetConnectionId(), CacheEntity::kMetadata, cache_access);
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
	const SanitizedCachePath cache_key {disk_cache_handle.internal_file_handle->GetPath()};
	auto metadata = metadata_cache->GetOrCreate(
	    cache_key, [this, &disk_cache_handle, &metadata_cache_hit](const string & /*unused*/) {
		    metadata_cache_hit = false;
		    return Stats(*disk_cache_handle.internal_file_handle);
	    });
	const CacheAccess cache_access = metadata_cache_hit ? CacheAccess::kCacheHit : CacheAccess::kCacheMiss;
	RecordCacheAccess(disk_cache_handle.GetConnectionId(), CacheEntity::kMetadata, cache_access);
	return metadata->file_size;
}
int64_t CacheFileSystem::ReadImpl(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	const auto file_size = GetFileSize(handle);

	// No more bytes to read.
	if (location >= static_cast<idx_t>(file_size)) {
		return 0;
	}

	auto state = instance_state.lock();

	// If the source filepath matches exclusion regex, skip caching.
	const int64_t bytes_to_read = MinValue<int64_t>(nr_bytes, file_size - location);
	if (state->exclusion_manager.MatchAnyExclusion(handle.GetPath())) {
		auto &cache_handle = handle.Cast<CacheFileSystemHandle>();
		internal_filesystem->Read(*cache_handle.internal_file_handle, buffer, nr_bytes, location);
		return bytes_to_read;
	}

	state->cache_reader_manager.GetCacheReader()->ReadAndCache(handle, static_cast<char *>(buffer), location,
	                                                           bytes_to_read, file_size);
	return bytes_to_read;
}

void CacheFileSystem::RecordCacheAccess(connection_t conn_id, CacheEntity cache_entity, CacheAccess cache_access) {
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, conn_id);
	collector.RecordCacheAccess(cache_entity, cache_access);
}

void CacheFileSystem::RecordCacheAccess(connection_t conn_id, CacheEntity cache_entity, CacheAccess cache_access,
                                        idx_t byte_count) {
	auto state = instance_state.lock();
	auto &collector = GetProfileCollectorOrThrow(state, conn_id);
	collector.RecordCacheAccess(cache_entity, cache_access, byte_count);
}

} // namespace duckdb

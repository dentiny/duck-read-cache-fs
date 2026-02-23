#include "disk_cache_util.hpp"

#include "cache_filesystem_config.hpp"
#include "crypto.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "utils/include/filesystem_utils.hpp"
#include "utils/include/hash_utils.hpp"
#include "utils/include/resize_uninitialized.hpp"
#include "utils/include/url_utils.hpp"

namespace duckdb {

namespace {

// Get temporary cache filepath from the destination cache filepath.
string GetTempCacheFilePath(const string &cache_filepath) {
	// Dump to a unique temporary location at local filesystem, since there could be multiple processes writing cache
	// file for the same remote file.
	return StringUtil::Format("%s.%s.httpfs_local_cache", cache_filepath, UUID::ToString(UUID::GenerateRandomUUID()));
}

// Suffix for temporary cache files.
constexpr const char *TEMP_CACHE_FILE_SUFFIX = ".httpfs_local_cache";

// Staleness threshold for dead temp file cleanup: 10 minutes in microseconds.
constexpr idx_t DEAD_TEMP_STALENESS_MICROSEC = 10 * 60 * 1000 * 1000;

// xattr key prefix for storing chunked original cache filepath.
// Zero-padded indices (e.g. .001, .002) ensure lexicographic sort matches chunk order.
constexpr const char *CACHE_FILEPATH_ATTR_PREFIX = "user.cache_httpfs_cache_filepath";

// Try to reconstruct the original (non-shortened) local cache filepath from chunked xattr entries.
// Returns empty string if no attributes are found (i.e. the filepath was never shortened).
string TryGetOriginalCacheFilepath(const string &filepath) {
	string result;
	for (idx_t idx = 0;; ++idx) {
		const string key = StringUtil::Format("%s.%03llu", CACHE_FILEPATH_ATTR_PREFIX, idx);
		string chunk = GetFileAttribute(filepath, key);
		if (chunk.empty()) {
			break;
		}
		result += chunk;
	}
	return result;
}

} // namespace

/*static*/ DiskCacheUtil::CacheFileDestination DiskCacheUtil::GetLocalCacheFile(const vector<string> &cache_directories,
                                                                                const string &remote_file,
                                                                                idx_t start_offset,
                                                                                idx_t bytes_to_read) {
	D_ASSERT(!cache_directories.empty());

	const SanitizedCachePath cache_key {remote_file};
	duckdb::hash_bytes remote_file_sha256_val;
	static_assert(sizeof(remote_file_sha256_val) == 32);
	duckdb::sha256(cache_key.Path().data(), cache_key.Path().length(), remote_file_sha256_val);
	const string remote_file_sha256_str = GetSha256(cache_key.Path());
	const string fname = StringUtil::GetFileName(cache_key.Path());

	uint64_t hash_value = 0xcbf29ce484222325; // FNV offset basis
	for (idx_t idx = 0; idx < sizeof(remote_file_sha256_val); ++idx) {
		hash_value ^= static_cast<uint64_t>(remote_file_sha256_val[idx]);
		hash_value *= 0x100000001b3; // FNV prime
	}
	const idx_t cache_directory_idx = hash_value % cache_directories.size();
	const auto &cur_cache_dir = cache_directories[cache_directory_idx];
	if (StringUtil::EndsWith(cur_cache_dir, "/")) {
		throw InternalException("Cache directory %s cannot ends with '/'", cur_cache_dir);
	}

	auto cache_filepath = StringUtil::Format("%s/%s-%s-%llu-%llu", cur_cache_dir, remote_file_sha256_str, fname,
	                                         start_offset, bytes_to_read);
	return DiskCacheUtil::CacheFileDestination {
	    .cache_directory_idx = cache_directory_idx,
	    .cache_filepath = std::move(cache_filepath),
	};
}

/*static*/ DiskCacheUtil::RemoteFileInfo DiskCacheUtil::GetRemoteFileInfo(const string &cache_filepath) {
	// The on-disk filename may have been shortened to a SHA256 hash when it exceeded platform limits. In that case the
	// original full local cache filepath is stored in chunked xattr entries.  Recover it first so the split below works
	// correctly.
	string local_filename = StringUtil::GetFileName(cache_filepath);
	const string original_filepath = TryGetOriginalCacheFilepath(cache_filepath);
	if (!original_filepath.empty()) {
		local_filename = StringUtil::GetFileName(original_filepath);
	}

	// [local_filename] is formatted as <hash>-<remote-fname>-<start-offset>-<block-size>
	vector<string> tokens = StringUtil::Split(local_filename, "-");
	D_ASSERT(tokens.size() >= 4);

	// Get tokens for remote paths.
	vector<string> remote_path_tokens;
	remote_path_tokens.reserve(tokens.size() - 3);

	for (idx_t idx = 1; idx < tokens.size() - 2; ++idx) {
		remote_path_tokens.emplace_back(std::move(tokens[idx]));
	}

	const string &start_offset_str = tokens[tokens.size() - 2];
	const string &block_size_str = tokens[tokens.size() - 1];
	const idx_t start_offset = StringUtil::ToUnsigned(start_offset_str);
	const idx_t block_size = StringUtil::ToUnsigned(block_size_str);

	return RemoteFileInfo {
	    .remote_filename = StringUtil::Join(remote_path_tokens, "/"),
	    .start_offset = start_offset,
	    .end_offset = start_offset + block_size,
	};
}

/*static*/ string DiskCacheUtil::GetLocalCacheFilePrefix(const string &remote_file) {
	const SanitizedCachePath cache_key {remote_file};
	duckdb::hash_bytes remote_file_sha256_val;
	duckdb::sha256(cache_key.Path().data(), cache_key.Path().length(), remote_file_sha256_val);
	const string remote_file_sha256_str = Sha256ToHexString(remote_file_sha256_val);

	const string fname = StringUtil::GetFileName(cache_key.Path());
	return StringUtil::Format("%s-%s", remote_file_sha256_str, fname);
}

/*static*/ void DiskCacheUtil::EvictCacheFiles(FileSystem &local_filesystem, const string &cache_directory,
                                               const string &eviction_policy,
                                               const std::function<string()> &lru_eviction_decider) {
	// After cache file eviction and file deletion request we cannot perform a cache dump operation immediately,
	// because on unix platform files are only deleted physically when their last reference count goes away.
	//
	// For timestamp-based eviction, we simply return all the files which reaches certain threshold.
	if (eviction_policy == *ON_DISK_CREATION_TIMESTAMP_EVICTION) {
		EvictStaleCacheFiles(local_filesystem, cache_directory);
		return;
	}

	// For LRU-based eviction, get the entry to remove and delete the file to release storage space.
	D_ASSERT(eviction_policy == *ON_DISK_LRU_SINGLE_PROC_EVICTION);
	const auto filepath_to_evict = lru_eviction_decider();
	// Intentionally ignore return value.
	local_filesystem.TryRemoveFile(filepath_to_evict);
}

/*static*/ void DiskCacheUtil::StoreLocalCacheFile(const string &cache_directory,
                                                   const LocalCacheDestination &cache_dest, const string &content,
                                                   const string &version_tag, const InstanceConfig &config,
                                                   const std::function<string()> &lru_eviction_decider) {
	LocalFileSystem local_filesystem {};

	// Skip local cache if insufficient disk space.
	// It's worth noting it's not a strict check since there could be concurrent check and write operation (RMW
	// operation), but it's acceptable since min available disk space reservation is an order of magnitude bigger than
	// cache chunk size.
	if (!CanCacheOnDisk(cache_directory, config.cache_block_size, config.min_disk_bytes_for_cache)) {
		EvictCacheFiles(local_filesystem, cache_directory, config.on_disk_eviction_policy, lru_eviction_decider);
		return;
	}

	// Dump to a unique temporary location at local filesystem, since there could be multiple processes writing cache
	// file for the same remote file.
	{
		auto file_open_flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW;
		// When we enable write-through/read-through cache for disk cache reader, use direct IO to avoid double caching.
		// Notice direct IO requires IO size to be aligned with page size.
		if (config.enable_disk_reader_mem_cache && content.length() % GetFileSystemPageSize() == 0) {
			file_open_flags |= FileOpenFlags::FILE_FLAGS_DIRECT_IO;
		}
		auto file_handle = local_filesystem.OpenFile(cache_dest.temp_local_filepath, file_open_flags);
		local_filesystem.Write(*file_handle, const_cast<char *>(content.data()),
		                       /*nr_bytes=*/content.length(),
		                       /*location=*/0);
		file_handle->Sync();
	}

	// Then atomically move to the target postion to prevent data corruption due to concurrent write.
	//
	// TODO(hjiang): Provide a way to cleanup temporary files.
	// Issue reference: https://github.com/dentiny/duck-read-cache-fs/issues/422
	local_filesystem.MoveFile(/*source=*/cache_dest.temp_local_filepath,
	                          /*target=*/cache_dest.dest_local_filepath);

	// Store version tag in extended attributes for validation.
	if (!version_tag.empty()) {
		SetCacheVersion(cache_dest.dest_local_filepath, version_tag);
	}

	// Store chunked filepath attributes for original local cache access.
	if (!cache_dest.file_attrs.empty()) {
		SetFileAttributes(cache_dest.dest_local_filepath, cache_dest.file_attrs);
	}
}

/*static*/ DiskCacheUtil::LocalCacheReadResult DiskCacheUtil::ReadLocalCacheFile(const string &cache_filepath,
                                                                                 idx_t chunk_size, bool use_direct_io,
                                                                                 const string &version_tag) {
	auto file_open_flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS;
	if (use_direct_io) {
		file_open_flags |= FileOpenFlags::FILE_FLAGS_DIRECT_IO;
	}

	LocalFileSystem local_filesystem {};
	// On all platform, DuckDB opens option guarantee reference counting, which means even if the file is requested to
	// delete, already-opened file handle could still be accessed with no problem.
	// Reference: https://github.com/duckdb/duckdb/pull/19782
	auto file_handle = local_filesystem.OpenFile(cache_filepath, file_open_flags);

	// Check cache validity and clear if necessary.
	if (file_handle != nullptr && !ValidateCacheFile(cache_filepath, version_tag)) {
		local_filesystem.TryRemoveFile(cache_filepath);
		file_handle = nullptr;
	}

	if (file_handle == nullptr) {
		return LocalCacheReadResult {};
	}

	auto content = CreateResizeUninitializedString(chunk_size);
	local_filesystem.Read(*file_handle, const_cast<char *>(content.data()), chunk_size, /*location=*/0);

	// Update access and modification timestamp for the cache file, so it won't get evicted.
	// Intentionally ignore the return value, since it's possible the cache file has been requested to
	// delete by another eviction thread.
	UpdateFileTimestamps(cache_filepath);

	return LocalCacheReadResult {
	    .cache_hit = true,
	    .content = std::move(content),
	};
}

/*static*/ bool DiskCacheUtil::ValidateCacheFile(const string &cache_filepath, const string &version_tag) {
	// Empty version tags means cache validation is disabled.
	if (version_tag.empty()) {
		return true;
	}
	const string cached_version_tag = GetCacheVersion(cache_filepath);
	// If cached version tag is empty, the file might have been created before validation was enabled.
	// In this case, we should treat it as invalid to be safe.
	if (cached_version_tag.empty()) {
		return false;
	}
	return cached_version_tag == version_tag;
}

/*static*/ DiskCacheUtil::LocalCacheDestination
DiskCacheUtil::ResolveLocalCacheDestination(const string &cache_directory, const string &local_cache_file) {
	if (StringUtil::EndsWith(cache_directory, "/")) {
		throw InvalidInputException("Cache directory %s cannot ends with '/'", cache_directory);
	}

	const auto filepath_limit = GetMaxFileNameLength();
	auto local_temp_filepath = GetTempCacheFilePath(local_cache_file);
	auto local_temp_filename = StringUtil::GetFileName(local_temp_filepath);

	// If there's no oversized filepath or filename, no special handling.
	if (local_temp_filename.length() <= filepath_limit.max_filename_len &&
	    local_temp_filepath.length() <= filepath_limit.max_filepath_len) {
		LocalCacheDestination local_cache_dest {
		    .dest_local_filepath = local_cache_file,
		    .temp_local_filepath = std::move(local_temp_filepath),
		    .file_attrs = {},
		};
		return local_cache_dest;
	}

	// Otherwise, special handle oversized filepath and filename: use the hash value as filename.
	auto local_cache_filepath_sha256 = GetSha256(local_cache_file);
	LocalCacheDestination local_cache_dest;
	local_cache_dest.dest_local_filepath = StringUtil::Format("%s/%s", cache_directory, local_cache_filepath_sha256);
	local_cache_dest.temp_local_filepath = GetTempCacheFilePath(local_cache_dest.dest_local_filepath);

	// Store the original cache filepath in file attributes as chunked entries, since xattr values have
	// platform-specific size limits.
	const idx_t max_xattr_val_size = GetMaxXattrValueSize();
	const idx_t filepath_len = local_cache_file.length();
	const idx_t chunk_count = (filepath_len + max_xattr_val_size - 1) / max_xattr_val_size;

	local_cache_dest.file_attrs.reserve(chunk_count);
	for (idx_t idx = 0; idx < chunk_count; ++idx) {
		const idx_t offset = idx * max_xattr_val_size;
		const idx_t len = MinValue(max_xattr_val_size, filepath_len - offset);
		const string key = StringUtil::Format("%s.%03llu", CACHE_FILEPATH_ATTR_PREFIX, idx);
		local_cache_dest.file_attrs[key] = local_cache_file.substr(offset, len);
	}

	return local_cache_dest;
}

/*static*/ idx_t DiskCacheUtil::CleanupDeadTempFiles(const vector<string> &cache_directories) {
	LocalFileSystem local_filesystem {};
	idx_t deleted_count = 0;
	const timestamp_t now = Timestamp::GetCurrentTimestamp();

	for (const auto &cache_directory : cache_directories) {
		local_filesystem.ListFiles(cache_directory, [&local_filesystem, &cache_directory, &deleted_count,
		                                             now](const string &fname, bool /*unused*/) {
			if (!StringUtil::EndsWith(fname, TEMP_CACHE_FILE_SUFFIX)) {
				return;
			}
			const string full_path = StringUtil::Format("%s/%s", cache_directory, fname);
			auto file_handle = local_filesystem.OpenFile(full_path, FileOpenFlags::FILE_FLAGS_READ |
			                                                            FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
			if (file_handle == nullptr) {
				return;
			}
			const timestamp_t last_mod_time = local_filesystem.GetLastModifiedTime(*file_handle);
			const idx_t diff_microsec = static_cast<idx_t>(now.value - last_mod_time.value);
			if (diff_microsec >= DEAD_TEMP_STALENESS_MICROSEC && local_filesystem.TryRemoveFile(full_path)) {
				++deleted_count;
			}
		});
	}
	return deleted_count;
}

} // namespace duckdb

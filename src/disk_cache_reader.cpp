// Design notes on concurrent access for local cache files:
// - There could be multiple threads accessing one local cache file, some of them try to open and read, while others
// trying to delete if the file is stale;
// - To avoid data race (open the file after deletion), read threads should open the file directly, instead of check
// existence and open, which guarantees even the file get deleted due to staleness, read threads still get a snapshot.

#include "cache_filesystem.hpp"
#include "cache_filesystem_logger.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "cache_read_chunk.hpp"
#include "crypto.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "utils/include/chunk_utils.hpp"
#include "utils/include/filesystem_utils.hpp"
#include "utils/include/resize_uninitialized.hpp"
#include "utils/include/thread_pool.hpp"
#include "utils/include/thread_utils.hpp"

#include <cstdint>
#include <tuple>
#include <utility>

namespace duckdb {

namespace {

// Cache directory and cache filepath for certain request.
struct CacheFileDestination {
	// Index for all cache directories.
	idx_t cache_directory_idx = 0;
	// Local cache filepath.
	string cache_filepath;
};

// Convert SHA256 value to hex string.
string Sha256ToHexString(const duckdb::hash_bytes &sha256) {
	static constexpr char kHexChars[] = "0123456789abcdef";
	string result;
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
// Cache filename is formatted as `<cache-directory>/<filename-sha256>-<filename>-<start-offset>-<size>`. So we could
// get all cache files under one directory, and get all cache files with commands like `ls`.
//
// Considering the naming format, it's worth noting it might _NOT_ work for local files, including mounted filesystems.
CacheFileDestination GetLocalCacheFile(const vector<string> &cache_directories, const string &remote_file,
                                       idx_t start_offset, idx_t bytes_to_read) {
	D_ASSERT(!cache_directories.empty());

	duckdb::hash_bytes remote_file_sha256_val;
	static_assert(sizeof(remote_file_sha256_val) == 32);
	duckdb::sha256(remote_file.data(), remote_file.length(), remote_file_sha256_val);
	const string remote_file_sha256_str = Sha256ToHexString(remote_file_sha256_val);
	const string fname = StringUtil::GetFileName(remote_file);

	uint64_t hash_value = 0xcbf29ce484222325; // FNV offset basis
	for (idx_t idx = 0; idx < sizeof(remote_file_sha256_val); ++idx) {
		hash_value ^= static_cast<uint64_t>(remote_file_sha256_val[idx]);
		hash_value *= 0x100000001b3; // FNV prime
	}
	const idx_t cache_directory_idx = hash_value % cache_directories.size();
	const auto &cur_cache_dir = cache_directories[cache_directory_idx];

	auto cache_filepath = StringUtil::Format("%s/%s-%s-%llu-%llu", cur_cache_dir, remote_file_sha256_str, fname,
	                                         start_offset, bytes_to_read);
	return CacheFileDestination {
	    .cache_directory_idx = cache_directory_idx,
	    .cache_filepath = std::move(cache_filepath),
	};
}

// Get remote file information from the given local cache [fname].
std::tuple<string /*remote_filename*/, uint64_t /*start_offset*/, uint64_t /*end_offset*/>
GetRemoteFileInfo(const string &fname) {
	// [fname] is formatted as <hash>-<remote-fname>-<start-offset>-<block-size>
	vector<string> tokens = StringUtil::Split(fname, "-");
	D_ASSERT(tokens.size() >= 4);

	// Get tokens for remote paths.
	vector<string> remote_path_tokens;
	remote_path_tokens.reserve(tokens.size() - 3);

	for (idx_t idx = 1; idx < tokens.size() - 2; ++idx) {
		remote_path_tokens.emplace_back(std::move(tokens[idx]));
	}
	string remote_filename = StringUtil::Join(remote_path_tokens, "/");

	const string &start_offset_str = tokens[tokens.size() - 2];
	const string &block_size_str = tokens[tokens.size() - 1];
	const idx_t start_offset = StringUtil::ToUnsigned(start_offset_str);
	const idx_t block_size = StringUtil::ToUnsigned(block_size_str);

	return std::make_tuple(std::move(remote_filename), start_offset, start_offset + block_size);
}

// Used to delete on-disk cache files, which returns the file prefix for the given [remote_file].
string GetLocalCacheFilePrefix(const string &remote_file) {
	duckdb::hash_bytes remote_file_sha256_val;
	duckdb::sha256(remote_file.data(), remote_file.length(), remote_file_sha256_val);
	const string remote_file_sha256_str = Sha256ToHexString(remote_file_sha256_val);

	const string fname = StringUtil::GetFileName(remote_file);
	return StringUtil::Format("%s-%s", remote_file_sha256_str, fname);
}

// Attempt to evict cache files, if file size threshold reached.
void EvictCacheFiles(DiskCacheReader &reader, FileSystem &local_filesystem, const string &cache_directory,
                     const string &eviction_policy) {
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
	const auto filepath_to_evict = reader.EvictCacheBlockLru();
	// Intentionally ignore return value.
	local_filesystem.TryRemoveFile(filepath_to_evict);
}

} // namespace

DiskCacheReader::DiskCacheReader(ProfileCollectorManager &profile_collector_manager_p,
                                 weak_ptr<CacheHttpfsInstanceState> instance_state_p)
    : BaseCacheReader(profile_collector_manager_p), local_filesystem(LocalFileSystem::CreateLocal()),
      instance_state(std::move(instance_state_p)) {
}

string DiskCacheReader::EvictCacheBlockLru() {
	const std::lock_guard<std::mutex> lck(cache_file_creation_timestamp_map_mutex);
	// Initialize file creation timestamp map, which should be called only once.
	// IO operation is performed inside of critical section intentionally, since it's required for all threads.
	if (cache_file_creation_timestamp_map.empty()) {
		auto instance_state_locked = GetInstanceConfig(instance_state);
		const auto &cache_directories = instance_state_locked->config.on_disk_cache_directories;
		cache_file_creation_timestamp_map = GetOnDiskFilesUnder(cache_directories);
	}
	D_ASSERT(!cache_file_creation_timestamp_map.empty());

	auto filepath = std::move(cache_file_creation_timestamp_map.begin()->second);
	cache_file_creation_timestamp_map.erase(cache_file_creation_timestamp_map.begin());
	return filepath;
}

bool DiskCacheReader::ValidateCacheFile(const string &cache_filepath, const string &version_tag) {
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

bool DiskCacheReader::ValidateCacheEntry(InMemCacheEntry *cache_entry, const string &version_tag) {
	// Empty version tags means cache validation is disabled.
	if (version_tag.empty()) {
		return true;
	}
	return cache_entry->version_tag == version_tag;
}

void DiskCacheReader::CacheLocal(const FileHandle &handle, const string &cache_directory,
                                 const string &local_cache_file, const string &content, const string &version_tag) {
	auto instance_state_locked = GetInstanceConfig(instance_state);
	const auto &config = instance_state_locked->config;

	// Skip local cache if insufficient disk space.
	// It's worth noting it's not a strict check since there could be concurrent check and write operation (RMW
	// operation), but it's acceptable since min available disk space reservation is an order of magnitude bigger than
	// cache chunk size.
	if (!CanCacheOnDisk(cache_directory, config.cache_block_size, config.min_disk_bytes_for_cache)) {
		EvictCacheFiles(*this, *local_filesystem, cache_directory, config.on_disk_eviction_policy);
		return;
	}

	// Dump to a temporary location at local filesystem.
	const auto fname = StringUtil::GetFileName(handle.GetPath());
	const auto local_temp_file = StringUtil::Format("%s%s.%s.httpfs_local_cache", cache_directory, fname,
	                                                UUID::ToString(UUID::GenerateRandomUUID()));
	{
		auto file_open_flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW;
		// When we enable write-through/read-through cache for disk cache reader, use direct IO to avoid double caching.
		if (config.enable_disk_reader_mem_cache) {
			file_open_flags |= FileOpenFlags::FILE_FLAGS_DIRECT_IO;
		}
		auto file_handle = local_filesystem->OpenFile(local_temp_file, file_open_flags);
		local_filesystem->Write(*file_handle, const_cast<char *>(content.data()),
		                        /*nr_bytes=*/content.length(),
		                        /*location=*/0);
		file_handle->Sync();
	}

	// Then atomically move to the target postion to prevent data corruption due to concurrent write.
	local_filesystem->MoveFile(/*source=*/local_temp_file, /*target=*/local_cache_file);
	//
	// Store version tag in extended attributes for validation.
	if (!version_tag.empty()) {
		SetCacheVersion(local_cache_file, version_tag);
	}
}

vector<DataCacheEntryInfo> DiskCacheReader::GetCacheEntriesInfo() const {
	vector<DataCacheEntryInfo> cache_entries_info;

	// Fill in in-memory cache blocks for on disk cache reader.
	if (in_mem_cache_blocks != nullptr) {
		auto keys = in_mem_cache_blocks->Keys();
		cache_entries_info.reserve(keys.size());
		for (auto &cur_key : keys) {
			cache_entries_info.emplace_back(DataCacheEntryInfo {
			    .cache_filepath = "(no disk cache)",
			    .remote_filename = std::move(cur_key.fname),
			    .start_offset = cur_key.start_off,
			    .end_offset = cur_key.start_off + cur_key.blk_size,
			    .cache_type = "in-mem-disk-cache",
			});
		}
	}

	// Fill in on disk cache entries.
	auto instance_state_locked = GetInstanceConfig(instance_state);
	const auto &cache_directories = instance_state_locked->config.on_disk_cache_directories;
	for (const auto &cur_cache_dir : cache_directories) {
		local_filesystem->ListFiles(cur_cache_dir,
		                            [&cache_entries_info, cur_cache_dir](const string &fname, bool /*unused*/) {
			                            auto remote_file_info = GetRemoteFileInfo(fname);
			                            cache_entries_info.emplace_back(DataCacheEntryInfo {
			                                .cache_filepath = StringUtil::Format("%s/%s", cur_cache_dir, fname),
			                                .remote_filename = std::get<0>(remote_file_info),
			                                .start_offset = std::get<1>(remote_file_info),
			                                .end_offset = std::get<2>(remote_file_info),
			                                .cache_type = "on-disk",
			                            });
		                            });
	}

	return cache_entries_info;
}

void DiskCacheReader::ProcessCacheReadChunk(FileHandle &handle, const InstanceConfig &config, const string &version_tag,
                                            CacheReadChunk cache_read_chunk) {
	SetThreadName("RdCachRdThd");

	// Attempt in-memory cache block first, so potentially we don't need to access disk storage.
	InMemCacheBlock block_key;
	block_key.fname = handle.GetPath();
	block_key.start_off = cache_read_chunk.aligned_start_offset;
	block_key.blk_size = cache_read_chunk.chunk_size;
	auto cache_destination = GetLocalCacheFile(config.on_disk_cache_directories, handle.GetPath(),
	                                           cache_read_chunk.aligned_start_offset, cache_read_chunk.chunk_size);

	if (in_mem_cache_blocks != nullptr) {
		auto cache_entry = in_mem_cache_blocks->Get(block_key);
		// Check cache entry validity and clear if necessary.
		if (cache_entry != nullptr && !ValidateCacheEntry(cache_entry.get(), version_tag)) {
			in_mem_cache_blocks->Delete(block_key);
			cache_entry = nullptr;
			local_filesystem->TryRemoveFile(cache_destination.cache_filepath);
		}
		if (cache_entry != nullptr) {
			profile_collector_manager.RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheHit);
			DUCKDB_LOG_READ_CACHE_HIT((handle));
			cache_read_chunk.CopyBufferToRequestedMemory(cache_entry->data);
			return;
		}
	}

	// Create cache content.
	auto content = CreateResizeUninitializedString(cache_read_chunk.chunk_size);
	void *addr = const_cast<char *>(content.data());

	// Attempt to open the file directly, so a successfully opened file handle won't be deleted by cleanup
	// thread and lead to data race.
	//
	// TODO(hjiang): With in-memory cache block involved, we could place disk write to background thread.
	auto file_open_flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS;
	// When we enable write-through/read-through cache for disk cache reader, use direct IO to avoid double
	// caching.
	if (config.enable_disk_reader_mem_cache) {
		file_open_flags |= FileOpenFlags::FILE_FLAGS_DIRECT_IO;
	}

	auto file_handle = local_filesystem->OpenFile(cache_destination.cache_filepath, file_open_flags);

	// Check cache validity and clear if necessary.
	if (file_handle != nullptr && !ValidateCacheFile(cache_destination.cache_filepath, version_tag)) {
		local_filesystem->TryRemoveFile(cache_destination.cache_filepath);
		file_handle = nullptr;
	}

	if (file_handle != nullptr) {
		profile_collector_manager.RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheHit);
		DUCKDB_LOG_READ_CACHE_HIT((handle));
		{
			const auto latency_guard = profile_collector_manager.RecordOperationStart(IoOperation::kDiskCacheRead);
			local_filesystem->Read(*file_handle, addr, cache_read_chunk.chunk_size, /*location=*/0);
		}
		cache_read_chunk.CopyBufferToRequestedMemory(content);

		// Update in-memory cache if applicable.
		if (in_mem_cache_blocks != nullptr) {
			InMemCacheEntry new_cache_entry {
			    .data = std::move(content),
			    .version_tag = version_tag,
			};
			in_mem_cache_blocks->Put(std::move(block_key),
			                         make_shared_ptr<InMemCacheEntry>(std::move(new_cache_entry)));
		}

		// Update access and modification timestamp for the cache file, so it won't get evicted.
		// Intentionally ignore the return value, since it's possible the cache file has been requested to
		// delete by another eviction thread.
		// TODO(hjiang): Revisit deadline-based eviction policy with in-memory cache involved.
		UpdateFileTimestamps(cache_destination.cache_filepath);
		return;
	}

	// We suffer a cache loss, fallback to remote access then local filesystem write.
	profile_collector_manager.RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheMiss);
	DUCKDB_LOG_READ_CACHE_MISS((handle));
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto *internal_filesystem = disk_cache_handle.GetInternalFileSystem();

	{
		const auto latency_guard = profile_collector_manager.RecordOperationStart(IoOperation::kRead);
		internal_filesystem->Read(*disk_cache_handle.internal_file_handle, addr, cache_read_chunk.chunk_size,
		                          cache_read_chunk.aligned_start_offset);
	}

	// Copy to destination buffer, if bytes are read into [content] buffer rather than user-provided buffer.
	cache_read_chunk.CopyBufferToRequestedMemory(content);

	// Attempt to cache file locally.
	// We're tolerate of local cache file write failure, which doesn't affect returned content correctness.
	try {
		const auto &cache_directory = config.on_disk_cache_directories[cache_destination.cache_directory_idx];
		CacheLocal(handle, cache_directory, cache_destination.cache_filepath, content, version_tag);

		// Update in-memory cache if applicable.
		if (in_mem_cache_blocks != nullptr) {
			InMemCacheEntry new_cache_entry {
			    .data = std::move(content),
			    .version_tag = version_tag,
			};
			in_mem_cache_blocks->Put(std::move(block_key),
			                         make_shared_ptr<InMemCacheEntry>(std::move(new_cache_entry)));
		}
	} catch (...) {
	}
}

void DiskCacheReader::ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                   idx_t requested_bytes_to_read, idx_t file_size) {
	if (requested_bytes_to_read == 0) {
		return;
	}

	auto instance_state_locked = GetInstanceConfig(instance_state);
	const auto &config = instance_state_locked->config;
	std::call_once(cache_init_flag, [this, &config]() {
		if (config.enable_disk_reader_mem_cache) {
			in_mem_cache_blocks = make_uniq<InMemCache>(config.disk_reader_max_mem_cache_timeout_millisec,
			                                            config.disk_reader_max_mem_cache_timeout_millisec);
		}
	});

	const idx_t block_size = config.cache_block_size;
	const ReadRequestParams read_params {
	    .requested_start_offset = requested_start_offset,
	    .requested_bytes_to_read = requested_bytes_to_read,
	    .block_size = block_size,
	};
	const ChunkAlignmentInfo alignment_info = CalculateChunkAlignment(read_params);

	// Indicate the memory address to copy to for each IO operation
	char *addr_to_write = buffer;
	// Used to calculate bytes to copy for last chunk.
	idx_t already_read_bytes = 0;
	// Threads to parallelly perform IO.

	const auto task_count = GetThreadCountForSubrequests(alignment_info.subrequest_count);
	ThreadPool io_threads(task_count);
	vector<std::future<void>> io_task_futures;
	io_task_futures.reserve(task_count);
	// Get file-level metadata once before processing chunks.
	string version_tag = config.enable_cache_validation ? handle.Cast<CacheFileSystemHandle>().GetVersionTag() : "";

	// To improve IO performance, we split requested bytes (after alignment) into multiple chunks and fetch them in
	// parallel.
	idx_t total_bytes_to_cache = 0;
	for (idx_t io_start_offset = alignment_info.aligned_start_offset;
	     io_start_offset <= alignment_info.aligned_last_chunk_offset; io_start_offset += block_size) {
		// No bytes to read for the last chunk.
		if (io_start_offset == file_size) {
			continue;
		}

		CacheReadChunk cache_read_chunk;
		cache_read_chunk.requested_start_addr = addr_to_write;
		cache_read_chunk.aligned_start_offset = io_start_offset;
		cache_read_chunk.requested_start_offset = requested_start_offset;

		// Implementation-wise, middle chunks are easy to handle -- read in [block_size], and copy the whole chunk
		// to the requested memory address; but the first and last chunk require special handling. For first chunk,
		// requested start offset might not be aligned with block size; for the last chunk, we might not need to
		// copy the whole [block_size] of memory.
		//
		// Case-1: If there's only one chunk, which serves as both the first chunk and the last one.
		if (io_start_offset == alignment_info.aligned_start_offset &&
		    io_start_offset == alignment_info.aligned_last_chunk_offset) {
			cache_read_chunk.chunk_size = MinValue<idx_t>(block_size, file_size - io_start_offset);
			cache_read_chunk.bytes_to_copy = requested_bytes_to_read;
		}
		// Case-2: First chunk.
		else if (io_start_offset == alignment_info.aligned_start_offset) {
			const idx_t delta_offset = requested_start_offset - alignment_info.aligned_start_offset;
			addr_to_write += block_size - delta_offset;
			already_read_bytes += block_size - delta_offset;

			cache_read_chunk.chunk_size = block_size;
			cache_read_chunk.bytes_to_copy = block_size - delta_offset;
		}
		// Case-3: Last chunk.
		else if (io_start_offset == alignment_info.aligned_last_chunk_offset) {
			cache_read_chunk.chunk_size = MinValue<idx_t>(block_size, file_size - io_start_offset);
			cache_read_chunk.bytes_to_copy = requested_bytes_to_read - already_read_bytes;
		}
		// Case-4: Middle chunks.
		else {
			addr_to_write += block_size;
			already_read_bytes += block_size;

			cache_read_chunk.bytes_to_copy = block_size;
			cache_read_chunk.chunk_size = block_size;
		}
		total_bytes_to_cache += cache_read_chunk.chunk_size;

		// Update read offset for next chunk read.
		requested_start_offset = io_start_offset + block_size;

		// Perform read operation in parallel.
		auto cur_task_future = io_threads.Push([this, &handle, &config, &version_tag, cache_read_chunk]() {
			ProcessCacheReadChunk(handle, config, version_tag, cache_read_chunk);
		});
		io_task_futures.emplace_back(std::move(cur_task_future));
	}

	// Block wait for all IO operations to complete.
	io_threads.Wait();
	for (auto &cur_task_future : io_task_futures) {
		cur_task_future.get();
	}

	// Record "bytes to read" and "bytes to cache".
	profile_collector_manager.RecordActualCacheRead(/*cache_size=*/total_bytes_to_cache,
	                                                /*actual_bytes=*/requested_bytes_to_read);
}

void DiskCacheReader::ClearCache() {
	auto instance_state_locked = GetInstanceConfig(instance_state);
	const auto &config = instance_state_locked->config;
	for (const auto &cur_cache_dir : config.on_disk_cache_directories) {
		local_filesystem->RemoveDirectory(cur_cache_dir);
		// Create an empty directory, otherwise later read access errors.
		local_filesystem->CreateDirectory(cur_cache_dir);
	}
	if (in_mem_cache_blocks != nullptr) {
		in_mem_cache_blocks->Clear();
	}
}

void DiskCacheReader::ClearCache(const string &fname) {
	const string cache_file_prefix = GetLocalCacheFilePrefix(fname);
	auto instance_state_locked = GetInstanceConfig(instance_state);
	const auto &config = instance_state_locked->config;
	for (const auto &cur_cache_dir : config.on_disk_cache_directories) {
		local_filesystem->ListFiles(cur_cache_dir, [&](const string &cur_file, bool /*unused*/) {
			if (StringUtil::StartsWith(cur_file, cache_file_prefix)) {
				const string filepath = StringUtil::Format("%s/%s", cur_cache_dir, cur_file);
				local_filesystem->RemoveFile(filepath);
			}
		});
	}
	if (in_mem_cache_blocks != nullptr) {
		in_mem_cache_blocks->Clear([&fname](const InMemCacheBlock &block) { return block.fname == fname; });
	}
}

} // namespace duckdb

// Design notes on concurrent access for local cache files:
// - There could be multiple threads accessing one local cache file, some of them try to open and read, while others
// trying to delete if the file is stale;
// - To avoid data race (open the file after deletion), read threads should open the file directly, instead of check
// existence and open, which guarantees even the file get deleted due to staleness, read threads still get a snapshot.

#include "cache_filesystem.hpp"
#include "cache_filesystem_logger.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "cache_read_chunk.hpp"
#include "disk_cache_reader.hpp"
#include "disk_cache_util.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "utils/include/chunk_utils.hpp"
#include "utils/include/filesystem_utils.hpp"
#include "utils/include/resize_uninitialized.hpp"
#include "utils/include/thread_pool.hpp"
#include "utils/include/thread_utils.hpp"
#include "utils/include/url_utils.hpp"

#include <cstdint>
#include <utility>

namespace duckdb {

DiskCacheReader::DiskCacheReader(weak_ptr<CacheHttpfsInstanceState> instance_state_p,
                                 BaseProfileCollector &profile_collector_p)
    : BaseCacheReader(profile_collector_p, *ON_DISK_CACHE_READER_NAME),
      local_filesystem(LocalFileSystem::CreateLocal()), instance_state(std::move(instance_state_p)) {
}

string DiskCacheReader::EvictCacheBlockLru() {
	const concurrency::lock_guard<concurrency::mutex> lck(cache_file_creation_timestamp_map_mutex);
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

bool DiskCacheReader::ValidateCacheEntry(InMemCacheEntry *cache_entry, const string &version_tag) {
	// Empty version tags means cache validation is disabled.
	if (version_tag.empty()) {
		return true;
	}
	return cache_entry->version_tag == version_tag;
}

// TODO(hjiang): For oversized filepath, both in-memory cache and on-disk cache stores resolved path, which uses SHA-256
// instead of original filename, likely we should do a translation here. On-disk cache stores original filepath in file
// attributes, in-memory cache should do the same thing.
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
			                            auto cache_filepath = StringUtil::Format("%s/%s", cur_cache_dir, fname);
			                            auto remote_file_info = DiskCacheUtil::GetRemoteFileInfo(cache_filepath);
			                            cache_entries_info.emplace_back(DataCacheEntryInfo {
			                                .cache_filepath = std::move(cache_filepath),
			                                .remote_filename = std::move(remote_file_info.remote_filename),
			                                .start_offset = remote_file_info.start_offset,
			                                .end_offset = remote_file_info.end_offset,
			                                .cache_type = "on-disk",
			                            });
		                            });
	}

	return cache_entries_info;
}

void DiskCacheReader::ProcessCacheReadChunk(FileHandle &handle, const InstanceConfig &config, const string &version_tag,
                                            CacheReadChunk cache_read_chunk) {
	SetThreadName("RdCachRdThd");

	// Resolve the on-disk cache path once up-front; use the resolved filepath as the unified key for both in-memory and
	// on-disk cache.
	auto cache_file =
	    DiskCacheUtil::GetLocalCacheFile(config.on_disk_cache_directories, handle.GetPath(),
	                                     cache_read_chunk.aligned_start_offset, cache_read_chunk.chunk_size);
	const auto &cache_directory = config.on_disk_cache_directories[cache_file.cache_directory_idx];
	auto cache_dest = DiskCacheUtil::ResolveLocalCacheDestination(cache_directory, cache_file.cache_filepath);

	const InMemCacheBlock block_key {handle.GetPath(), cache_read_chunk.aligned_start_offset,
	                                 cache_read_chunk.chunk_size};

	// Attempt in-memory cache first, so potentially we don't need to access disk storage.
	if (in_mem_cache_blocks != nullptr) {
		auto cache_entry = in_mem_cache_blocks->Get(block_key);
		if (cache_entry != nullptr && !ValidateCacheEntry(cache_entry.get(), version_tag)) {
			in_mem_cache_blocks->Delete(block_key);
			cache_entry = nullptr;
			local_filesystem->TryRemoveFile(cache_dest.dest_local_filepath);
		}
		if (cache_entry != nullptr) {
			GetProfileCollector().RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheHit);
			DUCKDB_LOG_READ_CACHE_HIT((handle));
			cache_read_chunk.CopyBufferToRequestedMemory(cache_entry->data);
			return;
		}
	}

	// Attempt to open and read local cache file directly, so a successfully opened file handle won't be
	// deleted by cleanup thread and lead to data race.
	//
	// TODO(hjiang): With in-memory cache block involved, we could place disk write to background thread.
	{
		const auto latency_guard = GetProfileCollector().RecordOperationStart(IoOperation::kDiskCacheRead);
		auto read_result =
		    DiskCacheUtil::ReadLocalCacheFile(cache_dest.dest_local_filepath, cache_read_chunk.chunk_size, version_tag);
		if (read_result.cache_hit) {
			GetProfileCollector().RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheHit);
			DUCKDB_LOG_READ_CACHE_HIT((handle));
			cache_read_chunk.CopyBufferToRequestedMemory(read_result.content);

			// Update in-memory cache if applicable.
			if (in_mem_cache_blocks != nullptr) {
				InMemCacheEntry new_cache_entry {
				    .data = std::move(read_result.content),
				    .version_tag = version_tag,
				};
				in_mem_cache_blocks->Put(block_key, make_shared_ptr<InMemCacheEntry>(std::move(new_cache_entry)));
			}
			return;
		}
	}

	// We suffer a cache loss, fallback to remote access then local filesystem write.
	GetProfileCollector().RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheMiss);
	DUCKDB_LOG_READ_CACHE_MISS((handle));
	auto content = CreateResizeUninitializedString(cache_read_chunk.chunk_size);
	void *addr = const_cast<char *>(content.data());
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto *internal_filesystem = disk_cache_handle.GetInternalFileSystem();

	{
		const auto latency_guard = GetProfileCollector().RecordOperationStart(IoOperation::kRead);
		internal_filesystem->Read(*disk_cache_handle.internal_file_handle, addr, cache_read_chunk.chunk_size,
		                          cache_read_chunk.aligned_start_offset);
	}

	// Copy to destination buffer, if bytes are read into [content] buffer rather than user-provided buffer.
	cache_read_chunk.CopyBufferToRequestedMemory(content);

	// Attempt to cache file locally.
	// We're tolerate of local cache file write failure, which doesn't affect returned content correctness.
	try {
		DiskCacheUtil::StoreLocalCacheFile(cache_directory, cache_dest, content, version_tag, config,
		                                   [this]() { return EvictCacheBlockLru(); });

		// Update in-memory cache if applicable.
		if (in_mem_cache_blocks != nullptr) {
			InMemCacheEntry new_cache_entry {
			    .data = std::move(content),
			    .version_tag = version_tag,
			};
			in_mem_cache_blocks->Put(block_key, make_shared_ptr<InMemCacheEntry>(std::move(new_cache_entry)));
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

	const auto task_count = GetThreadCountForSubrequests(alignment_info.subrequest_count, config.max_subrequest_count);
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
	GetProfileCollector().RecordActualCacheRead(/*cache_size=*/total_bytes_to_cache,
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
	// Delete on-disk files.
	vector<string> cache_files_to_remove;
	const string cache_file_prefix = DiskCacheUtil::GetLocalCacheFilePrefix(fname);
	auto instance_state_locked = GetInstanceConfig(instance_state);
	const auto &config = instance_state_locked->config;
	for (const auto &cur_cache_dir : config.on_disk_cache_directories) {
		local_filesystem->ListFiles(cur_cache_dir, [&](const string &cur_file, bool /*unused*/) {
			if (StringUtil::StartsWith(cur_file, cache_file_prefix)) {
				string filepath = StringUtil::Format("%s/%s", cur_cache_dir, cur_file);
				cache_files_to_remove.emplace_back(std::move(filepath));
			}
		});
	}

	const auto thread_num = std::min<size_t>(GetCpuCoreCount(), cache_files_to_remove.size());
	ThreadPool tp {thread_num};
	vector<std::future<void>> file_remove_futures;
	file_remove_futures.reserve(cache_files_to_remove.size());
	for (auto cur_cache_file : cache_files_to_remove) {
		auto fut = tp.Push([this, cur = std::move(cur_cache_file)]() { local_filesystem->TryRemoveFile(cur); });
		file_remove_futures.emplace_back(std::move(fut));
	}
	tp.Wait();
	for (auto &cur_fut : file_remove_futures) {
		cur_fut.get();
	}

	// Delete in-memory cache for on-disk cache files.
	if (in_mem_cache_blocks != nullptr) {
		const SanitizedCachePath cache_key {fname};
		// Start from the first block key for this file (ordered by fname, start_off, blk_size).
		const InMemCacheBlock start_key {cache_key.Path(), /*start_off=*/0, /*blk_size=*/0};
		in_mem_cache_blocks->Clear(
		    start_key, [&cache_key](const InMemCacheBlock &block) { return block.fname == cache_key.Path(); });
	}
}

} // namespace duckdb

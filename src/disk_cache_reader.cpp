// Design notes on concurrent access for local cache files:
// - There could be multiple threads accessing one local cache file, some of them try to open and read, while others
// trying to delete if the file is stale;
// - To avoid data race (open the file after deletion), read threads should open the file directly, instead of check
// existence and open, which guarantees even the file get deleted due to staleness, read threads still get a snapshot.

#include "cache_filesystem_logger.hpp"
#include "crypto.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "utils/include/filesystem_utils.hpp"
#include "utils/include/resize_uninitialized.hpp"
#include "utils/include/thread_pool.hpp"
#include "utils/include/thread_utils.hpp"

#include <cstdint>
#include <tuple>
#include <utility>
#include <utime.h>

namespace duckdb {

namespace {

// Cache directory and cache filepath for certain request.
struct CacheFileDestination {
	// Index for all cache directories.
	idx_t cache_directory_idx = 0;
	// Local cache filepath.
	string cache_filepath;
};

// All read requests are split into chunks, and executed in parallel.
// A [CacheReadChunk] represents a chunked IO request and its corresponding partial IO request.
//
// TODO(hjiang): Merge on-disk read chunk with in-memory one.
struct CacheReadChunk {
	// Requested memory address and file offset to read from for current chunk.
	char *requested_start_addr = nullptr;
	idx_t requested_start_offset = 0;
	// Block size aligned [requested_start_offset].
	idx_t aligned_start_offset = 0;

	// Number of bytes for the chunk for IO operations, apart from the last chunk it's always cache block size.
	idx_t chunk_size = 0;

	// Always allocate block size of memory for first and last chunk.
	// For middle chunks, [content] is not allocated, and bytes directly reading into [requested_start_addr] to save
	// memory allocation.
	string content;
	// Number of bytes to copy from [content] to requested memory address.
	idx_t bytes_to_copy = 0;

	// For first or last blocks, [content] is always allocated and bytes are read into [content] first then copied to
	// user-provided buffer. Otherwise (middle block), bytes are directly read into user-provided buffer to save memory
	// allocation.
	char *GetAddressToReadTo() const {
		return content.empty() ? requested_start_addr : const_cast<char *>(content.data());
	}

	// Copy from [content] to application-provided buffer.
	void CopyBufferToRequestedMemory() {
		if (!content.empty()) {
			const idx_t delta_offset = requested_start_offset - aligned_start_offset;
			std::memmove(requested_start_addr, const_cast<char *>(content.data()) + delta_offset, bytes_to_copy);
		}
	}

	// Copy from [buffer] to application-provided buffer.
	void CopyBufferToRequestedMemory(const std::string &buffer) {
		const idx_t delta_offset = requested_start_offset - aligned_start_offset;
		std::memmove(requested_start_addr, const_cast<char *>(buffer.data()) + delta_offset, bytes_to_copy);
	}

	// Take as memory buffer. [content] is not usable afterwards.
	string TakeAsBuffer() {
		if (!content.empty()) {
			auto buffer = std::move(content);
			return buffer;
		}
		string buffer {requested_start_addr, chunk_size};
		return buffer;
	}
};

// Convert SHA256 value to hex string.
string Sha256ToHexString(const duckdb::hash_bytes &sha256) {
	static constexpr char kHexChars[] = "0123456789abcdef";
	std::string result;
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
	for (int idx = 0; idx < sizeof(remote_file_sha256_val); ++idx) {
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
std::tuple<std::string /*remote_filename*/, uint64_t /*start_offset*/, uint64_t /*end_offset*/>
GetRemoteFileInfo(const std::string &fname) {
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
void EvictCacheFiles(DiskCacheReader &reader, FileSystem &local_filesystem, const string &cache_directory) {
	// After cache file eviction and file deletion request we cannot perform a cache dump operation immediately,
	// because on unix platform files are only deleted physically when their last reference count goes away.
	//
	// For timestamp-based eviction, we simply return all the files which reaches certain threshold.
	if (*g_on_disk_eviction_policy == *ON_DISK_CREATION_TIMESTAMP_EVICTION) {
		EvictStaleCacheFiles(local_filesystem, cache_directory);
		return;
	}

	// For LRU-based eviction, get the entry to remove and delete the file to release storage space.
	D_ASSERT(*g_on_disk_eviction_policy == *ON_DISK_LRU_SINGLE_PROC_EVICTION);
	const auto filepath_to_evict = reader.EvictCacheBlockLru();
	// Intentionally ignore return value.
	local_filesystem.TryRemoveFile(filepath_to_evict);
}

// Attempt to cache [chunk] to local filesystem, if there's sufficient disk space available.
void CacheLocal(DiskCacheReader &reader, const CacheReadChunk &chunk, FileSystem &local_filesystem,
                const FileHandle &handle, const string &cache_directory, const string &local_cache_file) {
	// Skip local cache if insufficient disk space.
	// It's worth noting it's not a strict check since there could be concurrent check and write operation (RMW
	// operation), but it's acceptable since min available disk space reservation is an order of magnitude bigger than
	// cache chunk size.
	if (!CanCacheOnDisk(cache_directory)) {
		EvictCacheFiles(reader, local_filesystem, cache_directory);
		return;
	}

	// Dump to a temporary location at local filesystem.
	const auto fname = StringUtil::GetFileName(handle.GetPath());
	const auto local_temp_file = StringUtil::Format("%s%s.%s.httpfs_local_cache", cache_directory, fname,
	                                                UUID::ToString(UUID::GenerateRandomUUID()));
	{
		auto file_handle = local_filesystem.OpenFile(
		    local_temp_file, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW |
		                         // We do our own LRU caching, so use direct IO to avoid double caching.
		                         FileOpenFlags::FILE_FLAGS_DIRECT_IO);
		local_filesystem.Write(*file_handle, chunk.GetAddressToReadTo(),
		                       /*nr_bytes=*/chunk.chunk_size,
		                       /*location=*/0);
		file_handle->Sync();
	}

	// Then atomically move to the target postion to prevent data corruption due to concurrent write.
	local_filesystem.MoveFile(/*source=*/local_temp_file,
	                          /*target=*/local_cache_file);
}

} // namespace

DiskCacheReader::DiskCacheReader() : local_filesystem(LocalFileSystem::CreateLocal()) {
}

string DiskCacheReader::EvictCacheBlockLru() {
	std::lock_guard<std::mutex> lck(cache_file_creation_timestamp_map_mutex);
	// Initialize file creation timestamp map, which should be called only once.
	// IO operation is performed inside of critical section intentionally, since it's required for all threads.
	if (cache_file_creation_timestamp_map.empty()) {
		cache_file_creation_timestamp_map = GetOnDiskFilesUnder(*g_on_disk_cache_directories);
	}
	D_ASSERT(!cache_file_creation_timestamp_map.empty());

	auto filepath = std::move(cache_file_creation_timestamp_map.begin()->second);
	cache_file_creation_timestamp_map.erase(cache_file_creation_timestamp_map.begin());
	return filepath;
}

vector<DataCacheEntryInfo> DiskCacheReader::GetCacheEntriesInfo() const {
	vector<DataCacheEntryInfo> cache_entries_info;
	for (const auto &cur_cache_dir : *g_on_disk_cache_directories) {
		local_filesystem->ListFiles(cur_cache_dir,
		                            [&cache_entries_info, cur_cache_dir](const std::string &fname, bool /*unused*/) {
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

void DiskCacheReader::ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                   idx_t requested_bytes_to_read, idx_t file_size) {
	std::call_once(cache_init_flag, [this]() {
		if (g_enable_disk_reader_mem_cache) {
			in_mem_cache_blocks = make_uniq<InMemCache>(g_disk_reader_max_mem_cache_block_count,
			                                            g_disk_reader_max_mem_cache_timeout_millisec);
		}
	});

	const idx_t block_size = g_cache_block_size;
	const idx_t aligned_start_offset = requested_start_offset / block_size * block_size;
	const idx_t aligned_last_chunk_offset =
	    (requested_start_offset + requested_bytes_to_read) / block_size * block_size;
	const idx_t subrequest_count = (aligned_last_chunk_offset - aligned_start_offset) / block_size + 1;

	// Indicate the meory address to copy to for each IO operation
	char *addr_to_write = buffer;
	// Used to calculate bytes to copy for last chunk.
	idx_t already_read_bytes = 0;
	// Threads to parallelly perform IO.
	ThreadPool io_threads(GetThreadCountForSubrequests(subrequest_count));

	// To improve IO performance, we split requested bytes (after alignment) into multiple chunks and fetch them in
	// parallel.
	for (idx_t io_start_offset = aligned_start_offset; io_start_offset <= aligned_last_chunk_offset;
	     io_start_offset += block_size) {
		CacheReadChunk cache_read_chunk;
		cache_read_chunk.requested_start_addr = addr_to_write;
		cache_read_chunk.aligned_start_offset = io_start_offset;
		cache_read_chunk.requested_start_offset = requested_start_offset;

		// Implementation-wise, middle chunks are easy to handle -- read in [block_size], and copy the whole chunk to
		// the requested memory address; but the first and last chunk require special handling. For first chunk,
		// requested start offset might not be aligned with block size; for the last chunk, we might not need to copy
		// the whole [block_size] of memory.
		//
		// Case-1: If there's only one chunk, which serves as both the first chunk and the last one.
		if (io_start_offset == aligned_start_offset && io_start_offset == aligned_last_chunk_offset) {
			cache_read_chunk.chunk_size = MinValue<idx_t>(block_size, file_size - io_start_offset);
			cache_read_chunk.content = CreateResizeUninitializedString(cache_read_chunk.chunk_size);
			cache_read_chunk.bytes_to_copy = requested_bytes_to_read;
		}
		// Case-2: First chunk.
		else if (io_start_offset == aligned_start_offset) {
			const idx_t delta_offset = requested_start_offset - aligned_start_offset;
			addr_to_write += block_size - delta_offset;
			already_read_bytes += block_size - delta_offset;

			cache_read_chunk.chunk_size = block_size;
			cache_read_chunk.content = CreateResizeUninitializedString(block_size);
			cache_read_chunk.bytes_to_copy = block_size - delta_offset;
		}
		// Case-3: Last chunk.
		else if (io_start_offset == aligned_last_chunk_offset) {
			cache_read_chunk.chunk_size = MinValue<idx_t>(block_size, file_size - io_start_offset);
			cache_read_chunk.content = CreateResizeUninitializedString(cache_read_chunk.chunk_size);
			cache_read_chunk.bytes_to_copy = requested_bytes_to_read - already_read_bytes;
		}
		// Case-4: Middle chunks.
		else {
			addr_to_write += block_size;
			already_read_bytes += block_size;

			cache_read_chunk.bytes_to_copy = block_size;
			cache_read_chunk.chunk_size = block_size;
		}

		// Update read offset for next chunk read.
		requested_start_offset = io_start_offset + block_size;

		// Perform read operation in parallel.
		//
		// TODO(hjiang): Refactor the thread function.
		io_threads.Push([this, &handle, block_size, cache_read_chunk = std::move(cache_read_chunk)]() mutable {
			SetThreadName("RdCachRdThd");

			// Attempt in-memory cache block first, so potentially we don't need to access disk storage.
			InMemCacheBlock block_key;
			block_key.fname = handle.GetPath();
			block_key.start_off = cache_read_chunk.aligned_start_offset;
			block_key.blk_size = cache_read_chunk.chunk_size;
			if (in_mem_cache_blocks != nullptr) {
				auto cache_block = in_mem_cache_blocks->Get(block_key);
				if (cache_block != nullptr) {
					profile_collector->RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheHit);
					DUCKDB_LOG_READ_CACHE_HIT((handle));
					cache_read_chunk.CopyBufferToRequestedMemory(*cache_block);
					return;
				}
			}

			// Check local cache first, see if we could do a cached read.
			auto cache_destination =
			    GetLocalCacheFile(*g_on_disk_cache_directories, handle.GetPath(), cache_read_chunk.aligned_start_offset,
			                      cache_read_chunk.chunk_size);

			// Attempt to open the file directly, so a successfully opened file handle won't be deleted by cleanup
			// thread and lead to data race.
			//
			// TODO(hjiang): With in-memory cache block involved, we could place disk write to background thread.
			auto file_handle = local_filesystem->OpenFile(
			    cache_destination.cache_filepath,
			    FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS |
			        // We do our own LRU caching, so use direct IO to avoid double caching.
			        FileOpenFlags::FILE_FLAGS_DIRECT_IO);
			if (file_handle != nullptr) {
				profile_collector->RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheHit);
				DUCKDB_LOG_READ_CACHE_HIT((handle));
				void *addr = !cache_read_chunk.content.empty() ? const_cast<char *>(cache_read_chunk.content.data())
				                                               : cache_read_chunk.requested_start_addr;
				{
					const auto latency_guard = profile_collector->RecordOperationStart(IoOperation::kDiskCacheRead);
					local_filesystem->Read(*file_handle, addr, cache_read_chunk.chunk_size, /*location=*/0);
				}
				cache_read_chunk.CopyBufferToRequestedMemory();

				// Update in-memory cache if applicable.
				if (in_mem_cache_blocks != nullptr) {
					in_mem_cache_blocks->Put(std::move(block_key),
					                         make_shared_ptr<string>(cache_read_chunk.TakeAsBuffer()));
				}

				// Update access and modification timestamp for the cache file, so it won't get evicted.
				// TODO(hjiang): Revisit deadline-based eviction policy with in-memory cache involved.
				const int ret_code = utime(cache_destination.cache_filepath.data(), /*times=*/nullptr);
				// It's possible the cache file has been requested to delete by eviction thread, so `ENOENT` is a
				// tolarable error.
				if (ret_code != 0 && errno != ENOENT) {
					throw IOException("Fails to update %s's access and modification timestamp because %s",
					                  cache_destination.cache_filepath, strerror(errno));
				}
				return;
			}

			// We suffer a cache loss, fallback to remote access then local filesystem write.
			profile_collector->RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheMiss);
			DUCKDB_LOG_READ_CACHE_MISS((handle));
			auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
			auto *internal_filesystem = disk_cache_handle.GetInternalFileSystem();

			{
				const auto latency_guard = profile_collector->RecordOperationStart(IoOperation::kRead);
				internal_filesystem->Read(*disk_cache_handle.internal_file_handle,
				                          cache_read_chunk.GetAddressToReadTo(), cache_read_chunk.chunk_size,
				                          cache_read_chunk.aligned_start_offset);
			}

			// Copy to destination buffer, if bytes are read into [content] buffer rather than user-provided buffer.
			cache_read_chunk.CopyBufferToRequestedMemory();

			// Attempt to cache file locally.
			const auto &cache_directory = (*g_on_disk_cache_directories)[cache_destination.cache_directory_idx];
			CacheLocal(*this, cache_read_chunk, *local_filesystem, handle, cache_directory,
			           cache_destination.cache_filepath);

			// Update in-memory cache if applicable.
			if (in_mem_cache_blocks != nullptr) {
				in_mem_cache_blocks->Put(std::move(block_key),
				                         make_shared_ptr<string>(cache_read_chunk.TakeAsBuffer()));
			}
		});
	}
	io_threads.Wait();
}

void DiskCacheReader::ClearCache() {
	for (const auto &cur_cache_dir : *g_on_disk_cache_directories) {
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
	for (const auto &cur_cache_dir : *g_on_disk_cache_directories) {
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

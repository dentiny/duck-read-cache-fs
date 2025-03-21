#include "duckdb/common/thread.hpp"
#include "crypto.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "in_memory_cache_reader.hpp"
#include "utils/include/resize_uninitialized.hpp"
#include "utils/include/filesystem_utils.hpp"
#include "utils/include/thread_pool.hpp"
#include "utils/include/thread_utils.hpp"

#include <cstdint>
#include <utility>
#include <utime.h>

namespace duckdb {

namespace {

// All read requests are split into chunks, and executed in parallel.
// A [CacheReadChunk] represents a chunked IO request and its corresponding partial IO request.
struct CacheReadChunk {
	// Requested memory address and file offset to read from for current chunk.
	char *requested_start_addr = nullptr;
	idx_t requested_start_offset = 0;
	// Block size aligned [requested_start_offset].
	idx_t aligned_start_offset = 0;

	// Number of bytes for the chunk for IO operations, apart from the last chunk it's always cache block size.
	idx_t chunk_size = 0;

	// Number of bytes to copy from [content] to requested memory address.
	idx_t bytes_to_copy = 0;

	// Copy from [content] to application-provided buffer.
	void CopyBufferToRequestedMemory(const ImmutableBuffer &buffer) {
		const idx_t delta_offset = requested_start_offset - aligned_start_offset;
		std::memmove(requested_start_addr, const_cast<char *>(buffer.data()) + delta_offset, bytes_to_copy);
	}
};

} // namespace

void InMemoryCacheReader::ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                       idx_t requested_bytes_to_read, idx_t file_size) {
	std::call_once(cache_init_flag, [this]() {
		cache = make_uniq<InMemCache>(g_max_in_mem_cache_block_count, g_in_mem_cache_block_timeout_millisec);
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

	// To improve IO performance, we split requested bytes (after alignment) into
	// multiple chunks and fetch them in parallel.
	for (idx_t io_start_offset = aligned_start_offset; io_start_offset <= aligned_last_chunk_offset;
	     io_start_offset += block_size) {
		CacheReadChunk cache_read_chunk;
		cache_read_chunk.requested_start_addr = addr_to_write;
		cache_read_chunk.aligned_start_offset = io_start_offset;
		cache_read_chunk.requested_start_offset = requested_start_offset;

		// Implementation-wise, middle chunks are easy to handle -- read in [block_size], and copy the whole chunk to
		// the requested memory address; but the first and last chunk require special handling.
		// For first chunk, requested start offset might not be aligned with block size; for the last chunk, we might
		// not need to copy the whole [block_size] of memory.
		//
		// Case-1: If there's only one chunk, which serves as both the first chunk and the last one.
		if (io_start_offset == aligned_start_offset && io_start_offset == aligned_last_chunk_offset) {
			cache_read_chunk.chunk_size = MinValue<idx_t>(block_size, file_size - io_start_offset);
			cache_read_chunk.bytes_to_copy = requested_bytes_to_read;
		}
		// Case-2: First chunk.
		else if (io_start_offset == aligned_start_offset) {
			const idx_t delta_offset = requested_start_offset - aligned_start_offset;
			addr_to_write += block_size - delta_offset;
			already_read_bytes += block_size - delta_offset;

			cache_read_chunk.chunk_size = block_size;
			cache_read_chunk.bytes_to_copy = block_size - delta_offset;
		}
		// Case-3: Last chunk.
		else if (io_start_offset == aligned_last_chunk_offset) {
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

		// Update read offset for next chunk read.
		requested_start_offset = io_start_offset + block_size;

		// Perform read operation in parallel.
		io_threads.Push([this, &handle, block_size, cache_read_chunk = std::move(cache_read_chunk)]() mutable {
			SetThreadName("RdCachRdThd");

			// Check local cache first, see if we could do a cached read.
			InMemCacheBlock block_key;
			block_key.fname = handle.GetPath();
			block_key.start_off = cache_read_chunk.aligned_start_offset;
			block_key.blk_size = cache_read_chunk.chunk_size;
			auto cache_block = cache->Get(block_key);

			if (!cache_block.empty()) {
				profile_collector->RecordCacheAccess(BaseProfileCollector::CacheEntity::kData,
				                                     BaseProfileCollector::CacheAccess::kCacheHit);
				cache_read_chunk.CopyBufferToRequestedMemory(cache_block);
				return;
			}

			// We suffer a cache loss, fallback to remote access then local filesystem write.
			profile_collector->RecordCacheAccess(BaseProfileCollector::CacheEntity::kData,
			                                     BaseProfileCollector::CacheAccess::kCacheMiss);
			ImmutableBuffer content {cache_read_chunk.chunk_size};
			auto &in_mem_cache_handle = handle.Cast<CacheFileSystemHandle>();
			auto *internal_filesystem = in_mem_cache_handle.GetInternalFileSystem();

			const string oper_id = profile_collector->GenerateOperId();
			profile_collector->RecordOperationStart(BaseProfileCollector::IoOperation::kRead, oper_id);
			internal_filesystem->Read(*in_mem_cache_handle.internal_file_handle, const_cast<char *>(content.data()),
			                          content.size(), cache_read_chunk.aligned_start_offset);
			profile_collector->RecordOperationEnd(BaseProfileCollector::IoOperation::kRead, oper_id);

			// Copy to destination buffer.
			cache_read_chunk.CopyBufferToRequestedMemory(content);

			// Attempt to cache file locally.
			cache->Put(std::move(block_key), std::move(content));
		});
	}
	io_threads.Wait();
}

vector<DataCacheEntryInfo> InMemoryCacheReader::GetCacheEntriesInfo() const {
	if (cache == nullptr) {
		return {};
	}

	auto keys = cache->Keys();
	vector<DataCacheEntryInfo> cache_entries_info;
	cache_entries_info.reserve(keys.size());
	for (auto &cur_key : keys) {
		cache_entries_info.emplace_back(DataCacheEntryInfo {
		    .cache_filepath = "(no disk cache)",
		    .remote_filename = std::move(cur_key.fname),
		    .start_offset = cur_key.start_off,
		    .end_offset = cur_key.start_off + cur_key.blk_size,
		    .cache_type = "in-mem",
		});
	}
	return cache_entries_info;
}

void InMemoryCacheReader::ClearCache() {
	if (cache != nullptr) {
		cache->Clear();
	}
}

void InMemoryCacheReader::ClearCache(const string &fname) {
	if (cache != nullptr) {
		cache->Clear([&fname](const InMemCacheBlock &block) { return block.fname == fname; });
	}
}

} // namespace duckdb

#include "in_memory_cache_reader.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_logger.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "cache_read_chunk.hpp"
#include "crypto.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "utils/include/chunk_utils.hpp"
#include "utils/include/resize_uninitialized.hpp"
#include "utils/include/thread_pool.hpp"
#include "utils/include/thread_utils.hpp"

#include <cstdint>
#include <utility>

namespace duckdb {

namespace {

// Runtime config for InMemoryCacheReader (fetched from instance state or defaults)
struct InMemoryCacheReaderConfig {
	idx_t max_cache_block_count = DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT;
	idx_t cache_block_timeout_millisec = DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC;
	idx_t cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
	uint64_t max_subrequest_count = DEFAULT_MAX_SUBREQUEST_COUNT;
	bool enable_cache_validation = DEFAULT_ENABLE_CACHE_VALIDATION;
};

// Get runtime config from instance state (returns copy with defaults if unavailable)
InMemoryCacheReaderConfig GetConfig(const CacheHttpfsInstanceState &instance_state) {
	return InMemoryCacheReaderConfig {
	    .max_cache_block_count = instance_state.config.max_in_mem_cache_block_count,
	    .cache_block_timeout_millisec = instance_state.config.in_mem_cache_block_timeout_millisec,
	    .cache_block_size = instance_state.config.cache_block_size,
	    .max_subrequest_count = instance_state.config.max_subrequest_count,
	    .enable_cache_validation = instance_state.config.enable_cache_validation,
	};
}

} // namespace

bool InMemoryCacheReader::ValidateCacheEntry(InMemCacheEntry *cache_entry, const string &version_tag) {
	// Empty version tags means cache validation is disabled.
	if (version_tag.empty()) {
		return true;
	}
	return cache_entry->version_tag == version_tag;
}

void InMemoryCacheReader::ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                       idx_t requested_bytes_to_read, idx_t file_size,
                                       BaseProfileCollector *profile_collector) {
	if (requested_bytes_to_read == 0) {
		return;
	}

	const auto config = GetConfig(*instance_state.lock());

	std::call_once(cache_init_flag, [this, &config]() {
		cache = make_uniq<InMemCache>(config.max_cache_block_count, config.cache_block_timeout_millisec);
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
	ThreadPool io_threads(GetThreadCountForSubrequests(alignment_info.subrequest_count));
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

		// Implementation-wise, middle chunks are easy to handle -- read in [block_size], and copy the whole chunk to
		// the requested memory address; but the first and last chunk require special handling.
		// For first chunk, requested start offset might not be aligned with block size; for the last chunk, we might
		// not need to copy the whole [block_size] of memory.
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
		io_threads.Push([this, &handle, version_tag = std::cref(version_tag), cache_read_chunk = cache_read_chunk,
		                 profile_collector]() mutable {
			SetThreadName("RdCachRdThd");

			// Check local cache first, see if we could do a cached read.
			InMemCacheBlock block_key;
			block_key.fname = handle.GetPath();
			block_key.start_off = cache_read_chunk.aligned_start_offset;
			block_key.blk_size = cache_read_chunk.chunk_size;
			auto cache_entry = cache->Get(block_key);

			// Check cache entry validity and clear if necessary.
			if (cache_entry != nullptr && !ValidateCacheEntry(cache_entry.get(), version_tag.get())) {
				cache->Delete(block_key);
				cache_entry = nullptr;
			}

			if (cache_entry != nullptr) {
				if (profile_collector) {
					profile_collector->RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheHit,
					                                     cache_read_chunk.bytes_to_copy);
				}
				DUCKDB_LOG_OPEN_CACHE_HIT((handle));
				cache_read_chunk.CopyBufferToRequestedMemory(cache_entry->data);
				return;
			}

			// We suffer a cache loss, fallback to remote access then local filesystem write.
			if (profile_collector) {
				profile_collector->RecordCacheAccess(CacheEntity::kData, CacheAccess::kCacheMiss,
				                                     cache_read_chunk.bytes_to_copy);
			}
			DUCKDB_LOG_OPEN_CACHE_MISS((handle));
			auto content = CreateResizeUninitializedString(cache_read_chunk.chunk_size);
			void *addr = const_cast<char *>(content.data());
			auto &in_mem_cache_handle = handle.Cast<CacheFileSystemHandle>();
			auto *internal_filesystem = in_mem_cache_handle.GetInternalFileSystem();

			if (profile_collector) {
				const auto latency_guard = profile_collector->RecordOperationStart(IoOperation::kRead);
				internal_filesystem->Read(*in_mem_cache_handle.internal_file_handle, addr, cache_read_chunk.chunk_size,
				                          cache_read_chunk.aligned_start_offset);
			} else {
				internal_filesystem->Read(*in_mem_cache_handle.internal_file_handle, addr, cache_read_chunk.chunk_size,
				                          cache_read_chunk.aligned_start_offset);
			}

			// Copy to destination buffer.
			cache_read_chunk.CopyBufferToRequestedMemory(content);

			InMemoryCacheReader::InMemCacheEntry new_cache_entry {
			    .data = std::move(content),
			    .version_tag = version_tag.get(),
			};
			cache->Put(std::move(block_key),
			           make_shared_ptr<InMemoryCacheReader::InMemCacheEntry>(std::move(new_cache_entry)));
		});
	}
	io_threads.Wait();

	// Record "bytes to read" and "bytes to cache".
	if (profile_collector) {
		profile_collector->RecordActualCacheRead(/*cache_size=*/total_bytes_to_cache,
		                                         /*actual_bytes=*/requested_bytes_to_read);
	}
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

// A filesystem wrapper, which performs in-memory cache for read operations.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "cache_filesystem_config.hpp"
#include "in_mem_cache_block.hpp"
#include "lru_cache.hpp"
#include "base_cache_filesystem.hpp"
#include "duckdb/common/file_opener.hpp"
#include "base_cache_reader.hpp"

namespace duckdb {

class InMemoryCacheReader final : public BaseCacheReader {
public:
	explicit InMemoryCacheReader(FileSystem *internal_filesystem_p) : BaseCacheReader(internal_filesystem_p) {
	}
	~InMemoryCacheReader() override = default;

	std::string GetName() const override {
		return "in_mem_cache_reader";
	}

	// Read from [handle] for an block-size aligned chunk into [start_addr]; cache
	// to local filesystem and return to user.
	void ReadAndCache(FileHandle &handle, char *buffer, uint64_t requested_start_offset,
	                  uint64_t requested_bytes_to_read, uint64_t file_size) override;

private:
	using InMemCache = ThreadSafeSharedLruCache<InMemCacheBlock, string, InMemCacheBlockHash, InMemCacheBlockEqual>;

	// LRU cache to store blocks; late initialized after first access.
	unique_ptr<InMemCache> cache;
};

} // namespace duckdb

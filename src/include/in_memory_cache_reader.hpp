// A filesystem wrapper, which performs in-memory cache for read operations.

#pragma once

#include "base_cache_reader.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "in_mem_cache_block.hpp"
#include "lru_cache.hpp"

namespace duckdb {

class InMemoryCacheReader final : public BaseCacheReader {
public:
	explicit InMemoryCacheReader(FileSystem *internal_filesystem_p) : BaseCacheReader(internal_filesystem_p) {
	}
	~InMemoryCacheReader() override = default;

	std::string GetName() const override {
		return "in_mem_cache_reader";
	}

	void ClearCache() override;
	void ReadAndCache(FileHandle &handle, char *buffer, uint64_t requested_start_offset,
	                  uint64_t requested_bytes_to_read, uint64_t file_size) override;

private:
	using InMemCache = ThreadSafeSharedLruCache<InMemCacheBlock, string, InMemCacheBlockHash, InMemCacheBlockEqual>;

	// Once flag to guard against cache's initialization.
	std::once_flag cache_init_flag;
	// LRU cache to store blocks; late initialized after first access.
	unique_ptr<InMemCache> cache;
};

} // namespace duckdb

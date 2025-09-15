// A filesystem wrapper, which performs on-disk cache for read operations.

#pragma once

#include <mutex>

#include "base_cache_reader.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"

namespace duckdb {

class DiskCacheReader final : public BaseCacheReader {
public:
	DiskCacheReader();
	~DiskCacheReader() override = default;

	std::string GetName() const override {
		return "on_disk_cache_reader";
	}

	void ClearCache() override;
	void ClearCache(const string &fname) override;

	void ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset, idx_t requested_bytes_to_read,
	                  idx_t file_size) override;

	vector<DataCacheEntryInfo> GetCacheEntriesInfo() const override;

	// Get file cache block to evict.
	// Notice returned filepath will be removed from LRU list, but the actual file won't be deleted.
	string EvictCacheBlockLru();

private:
	// Used to access local cache files.
	unique_ptr<FileSystem> local_filesystem;
	// Used for on-disk cache block LRU-based eviction.
	std::mutex cache_file_creation_timestamp_map_mutex;
	// Maps from last access timestamp to filepath.
	map<time_t, string> cache_file_creation_timestamp_map;
};

} // namespace duckdb

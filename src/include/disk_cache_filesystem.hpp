// A filesystem wrapper, which performs on-disk cache for read operations.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "cache_filesystem_config.hpp"
#include "base_cache_reader.hpp"
#include "base_cache_filesystem.hpp"

namespace duckdb {

class DiskCacheReader final : public BaseCacheReader {
public:
	explicit DiskCacheReader(FileSystem *internal_filesystem_p);
	~DiskCacheReader() override = default;

	std::string GetName() const override {
		return "on_disk_cache_reader";
	}

	// Read from [handle] for an block-size aligned chunk into [start_addr]; cache
	// to local filesystem and return to user.
	void ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset, idx_t requested_bytes_to_read,
	                  idx_t file_size) override;

private:
	// Used to access local cache files.
	unique_ptr<FileSystem> local_filesystem;
};

} // namespace duckdb

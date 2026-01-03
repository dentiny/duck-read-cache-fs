// A noop cache reader, which simply delegates IO request to filesystem API calls.
// - It provides an option for users to disable caching and parallel reads;
// - It eases performance comparison benchmarks.

#pragma once

#include "base_cache_reader.hpp"
#include "base_profile_collector.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class NoopCacheReader : public BaseCacheReader {
public:
	explicit NoopCacheReader(BaseProfileCollector &profile_collector_p)
	    : BaseCacheReader(profile_collector_p, *NOOP_CACHE_READER_NAME) {
	}
	~NoopCacheReader() override = default;

	void ClearCache() override {
	}
	void ClearCache(const string &fname) override {
	}
	void ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset, idx_t requested_bytes_to_read,
	                  idx_t file_size) override;

	vector<DataCacheEntryInfo> GetCacheEntriesInfo() const override {
		return {};
	}

	string GetName() const override {
		return *NOOP_CACHE_READER_NAME;
	}
};

} // namespace duckdb

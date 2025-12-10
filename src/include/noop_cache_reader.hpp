// A noop cache reader, which simply delegates IO request to filesystem API calls.
// - It provides an option for users to disable caching and parallel reads;
// - It eases performance comparison benchmarks.

#pragma once

#include "base_cache_reader.hpp"
#include "base_profile_collector.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class NoopCacheReader : public BaseCacheReader {
public:
	NoopCacheReader() = default;
	virtual ~NoopCacheReader() = default;

	void ClearCache() override {
	}
	void ClearCache(const string &fname) override {
	}
	void ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset, idx_t requested_bytes_to_read,
	                  idx_t file_size, BaseProfileCollector *profile_collector = nullptr) override;

	vector<DataCacheEntryInfo> GetCacheEntriesInfo() const override {
		return {};
	}

	std::string GetName() const override {
		return "noop_cache_reader";
	}
};

} // namespace duckdb

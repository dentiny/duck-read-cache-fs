// This class is the base class for reader implementation.

#pragma once

#include "base_cache_reader.hpp"
#include "base_profile_collector.hpp"
#include "cache_entry_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class BaseCacheReader {
public:
	BaseCacheReader(FileSystem *internal_filesystem_p) : internal_filesystem(internal_filesystem_p) {
	}
	virtual ~BaseCacheReader() = default;
	BaseCacheReader(const BaseCacheReader &) = delete;
	BaseCacheReader &operator=(const BaseCacheReader &) = delete;

	// Read from [handle] for an block-size aligned chunk into [start_addr]; cache to local filesystem and return to
	// user.
	virtual void ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
	                          idx_t requested_bytes_to_read, idx_t file_size) = 0;

	// Get status information for all cache entries for the current cache reader. Entries are returned in a random
	// order.
	virtual vector<CacheEntryInfo> GetCacheEntriesInfo() const = 0;

	// Clear all cache.
	virtual void ClearCache() = 0;

	// Clear cache for the given [fname].
	virtual void ClearCache(const string &fname) = 0;

	// Get name for cache reader.
	virtual std::string GetName() const {
		throw NotImplementedException("Base cache reader doesn't implement GetName.");
	}

	void SetProfileCollector(BaseProfileCollector *profile_collector_p) {
		profile_collector = profile_collector_p;
		profile_collector->SetCacheReaderType(GetName());
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	// Ownership lies in cache filesystem.
	FileSystem *internal_filesystem = nullptr;
	// Ownership lies in cache filesystem.
	BaseProfileCollector *profile_collector = nullptr;
};

} // namespace duckdb

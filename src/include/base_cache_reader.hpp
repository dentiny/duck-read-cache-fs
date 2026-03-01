// This class is the base class for reader implementation.
//
// All cache-related resource and operations are delegated to the corresponding cache reader.
// For example, access local cache files should go through on-disk cache reader.

#pragma once

#include "cache_entry_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Forward declaration.
struct CacheHttpfsInstanceState;

class BaseCacheReader {
public:
	explicit BaseCacheReader(weak_ptr<CacheHttpfsInstanceState> instance_state_p)
	    : instance_state(std::move(instance_state_p)) {
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
	virtual vector<DataCacheEntryInfo> GetCacheEntriesInfo() const = 0;

	// Clear all cache.
	virtual void ClearCache() = 0;

	// Clear cache for the given [fname].
	virtual void ClearCache(const string &fname) = 0;

	// Get name for cache reader.
	virtual string GetName() const {
		throw NotImplementedException("Base cache reader doesn't implement GetName.");
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
	// Ownership lies in cache httpfs instance state, which gets updated at extension setting update callback.
	// Refer to [CacheHttpfsInstanceState] for thread-safety guarentee.
	weak_ptr<CacheHttpfsInstanceState> instance_state;
};

} // namespace duckdb

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

	virtual void ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
	                          idx_t requested_bytes_to_read, idx_t file_size) = 0;

	virtual vector<DataCacheEntryInfo> GetCacheEntriesInfo() const = 0;

	virtual void ClearCache() = 0;

	virtual void ClearCache(const string &fname) = 0;

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
	weak_ptr<CacheHttpfsInstanceState> instance_state;
};

} // namespace duckdb

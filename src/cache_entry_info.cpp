#include "cache_entry_info.hpp"

#include <tuple>

namespace duckdb {

bool operator<(const DataCacheEntryInfo &lhs, const DataCacheEntryInfo &rhs) {
	return std::tie(lhs.cache_filepath, lhs.remote_filename, lhs.start_offset, lhs.end_offset, lhs.cache_type) <
	       std::tie(rhs.cache_filepath, rhs.remote_filename, rhs.start_offset, rhs.end_offset, rhs.cache_type);
}

bool operator<(const CacheAccessInfo &lhs, const CacheAccessInfo &rhs) {
	// Other fields are only specific to particular cache types.
	return std::tie(lhs.cache_type, lhs.cache_hit_count, lhs.cache_miss_count) <
	       std::tie(rhs.cache_type, rhs.cache_hit_count, rhs.cache_miss_count);
}

} // namespace duckdb

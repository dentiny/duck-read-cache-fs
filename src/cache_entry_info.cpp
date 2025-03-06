#include "cache_entry_info.hpp"

#include <tuple>

namespace duckdb {

bool operator<(const CacheEntryInfo &lhs, const CacheEntryInfo &rhs) {
	return std::tie(lhs.cache_filepath, lhs.remote_filename, lhs.start_offset, lhs.end_offset, lhs.cache_type) <
	       std::tie(rhs.cache_filepath, rhs.remote_filename, rhs.start_offset, rhs.end_offset, rhs.cache_type);
}

} // namespace duckdb

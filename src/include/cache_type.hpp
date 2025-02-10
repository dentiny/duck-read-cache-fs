#pragma once

#include "duckdb/common/file_opener.hpp"

namespace duckdb {

enum class CacheType {
	kInMem,
	kOnDisk,
	kUnknown,
};

// Get cache type from [opener] if specified.
// This function is read-only.
CacheType GetCacheType(optional_ptr<FileOpener> opener);

// Get cache type from its string version.
CacheType GetCacheType(const std::string &cache_type);

// Set global cache type if input is valid.
void SetGlobalCacheType(CacheType cache_type);

} // namespace duckdb

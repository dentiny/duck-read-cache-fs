#include "cache_type.hpp"
#include "cache_filesystem_config.hpp"

namespace duckdb {

CacheType GetCacheType(optional_ptr<FileOpener> opener) {
	if (opener == nullptr) {
		return g_cache_type == ON_DISK_CACHE_TYPE ? CacheType::kOnDisk : CacheType::kInMem;
	}
	Value val;
	FileOpener::TryGetCurrentSetting(opener, "cached_http_type", val);

	std::string requested_cache_type = val.ToString();
	if (requested_cache_type == IN_MEM_CACHE_TYPE) {
		return CacheType::kInMem;
	}
	if (requested_cache_type == ON_DISK_CACHE_TYPE) {
		return CacheType::kOnDisk;
	}
	return CacheType::kUnknown;
}

CacheType GetCacheType(const std::string &cache_type) {
	if (cache_type == ON_DISK_CACHE_TYPE) {
		return CacheType::kOnDisk;
	}
	if (cache_type == IN_MEM_CACHE_TYPE) {
		return CacheType::kInMem;
	}
	return CacheType::kUnknown;
}

void SetGlobalCacheType(CacheType cache_type) {
	switch (cache_type) {
	case CacheType::kInMem:
		g_cache_type = IN_MEM_CACHE_TYPE;
	case CacheType::kOnDisk:
		g_cache_type = ON_DISK_CACHE_TYPE;
	default:
		// Given cache type is invalid, do nothing.
		return;
	}
}

} // namespace duckdb

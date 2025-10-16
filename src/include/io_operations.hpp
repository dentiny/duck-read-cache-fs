#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace duckdb {

enum class CacheEntity {
	kMetadata,   // File metadata.
	kData,       // File data block.
	kFileHandle, // File handle.
	kGlob,       // Glob.
	kUnknown,
};
enum class CacheAccess {
	kCacheHit,
	kCacheMiss,
	// Used only for exclusive resource (i.e., file handle), which indicates a cache hit but cannot leverage cached
	// entry due to resource exclusiveness. Useful indicator to check whether high cache miss is caused by low cache
	// hit rate or small cache size.
	kCacheEntryInUse,
	kUnknown,
};
enum class IoOperation {
	kOpen,
	kRead,
	kGlob,
	// Disk cache read and copy.
	kDiskCacheRead,
	kUnknown,
};
inline constexpr auto kCacheEntityCount = static_cast<size_t>(CacheEntity::kUnknown);
inline constexpr auto kIoOperationCount = static_cast<size_t>(IoOperation::kUnknown);
inline constexpr auto kCacheAccessCount = static_cast<size_t>(CacheAccess::kUnknown);

// Some platforms supports musl libc, which doesn't support operator[], so we use `vector<>` instead of `constexpr
// static std::array<>` here.
//
// Cache entity name, indexed by cache entity enum.
//
// TODO(hjiang): Use constants for cache entity name and operation name.
inline const vector<const char *> CACHE_ENTITY_NAMES = []() {
	vector<const char *> cache_entity_names;
	cache_entity_names.reserve(kCacheEntityCount);
	cache_entity_names.emplace_back("metadata");
	cache_entity_names.emplace_back("data");
	cache_entity_names.emplace_back("file handle");
	cache_entity_names.emplace_back("glob");
	D_ASSERT(cache_entity_names.size() == kCacheEntityCount);
	return cache_entity_names;
}();
// Operation names, indexed by operation enums.
inline const vector<const char *> OPER_NAMES = []() {
	vector<const char *> oper_names;
	oper_names.reserve(kIoOperationCount);
	oper_names.emplace_back("open");
	oper_names.emplace_back("read");
	oper_names.emplace_back("glob");
	oper_names.emplace_back("disk_cache_read");
	D_ASSERT(oper_names.size() == kIoOperationCount);
	return oper_names;
}();

} // namespace duckdb

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "duckdb/common/vector.hpp"

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
	kWrite,
	kFileSync,
	kFileRemove,
	kGlob,
	// Disk cache read and copy.
	kDiskCacheRead,
	// Filepath cache clarance.
	kFilePathCacheClear,
	kUnknown,
};
constexpr size_t kCacheEntityCount = static_cast<size_t>(CacheEntity::kUnknown);
constexpr size_t kIoOperationCount = static_cast<size_t>(IoOperation::kUnknown);
constexpr size_t kCacheAccessCount = static_cast<size_t>(CacheAccess::kUnknown);

// Some platforms supports musl libc, which doesn't support operator[], so we use `vector<>` instead of `constexpr
// static std::array<>` here.
//
// Cache entity name, indexed by cache entity enum.
//
// TODO(hjiang): Use constants for cache entity name and operation name.
extern const vector<const char *> CACHE_ENTITY_NAMES;
// Operation names, indexed by operation enums.
extern const vector<const char *> OPER_NAMES;

} // namespace duckdb

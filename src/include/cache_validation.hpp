#pragma once

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

// Validate cache entry using version tag and last modification timestamp.
// Returns true if the cache entry is valid, false otherwise.
bool IsCacheEntryValid(bool validate, const string &cached_version_tag, timestamp_t cached_last_modified,
                       const string &current_version_tag, timestamp_t current_last_modified);

// Validate and potentially invalidate a cache entry.
// Returns the cache entry if still valid, nullptr if invalidated.
// This function handles getting current file metadata and checking validation.
template <typename CacheEntry, typename Cache, typename CacheKey>
shared_ptr<CacheEntry> ValidateCacheEntry(shared_ptr<CacheEntry> cache_entry, FileHandle &handle, Cache *cache,
                                          const CacheKey &block_key) {
	if (cache_entry == nullptr) {
		return nullptr;
	}

	// Get current file metadata for validation.
	auto &cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto *internal_filesystem = cache_handle.GetInternalFileSystem();
	const string current_version_tag = internal_filesystem->GetVersionTag(*cache_handle.internal_file_handle);
	const timestamp_t current_last_modified =
	    internal_filesystem->GetLastModifiedTime(*cache_handle.internal_file_handle);

	// Check if validation is enabled and if entry is valid.
	if (g_enable_cache_validation && !IsCacheEntryValid(true, cache_entry->version_tag, cache_entry->last_modified,
	                                                    current_version_tag, current_last_modified)) {
		// Cache entry is invalid, remove it.
		cache->Delete(block_key);
		return nullptr;
	}

	return cache_entry;
}

// Overload that accepts pre-fetched metadata to avoid duplicate calls.
template <typename CacheEntry, typename Cache, typename CacheKey>
shared_ptr<CacheEntry> ValidateCacheEntry(shared_ptr<CacheEntry> cache_entry, const string &current_version_tag,
                                          const timestamp_t &current_last_modified, Cache *cache,
                                          const CacheKey &block_key) {
	if (cache_entry == nullptr) {
		return nullptr;
	}

	// Check if validation is enabled and if entry is valid.
	if (g_enable_cache_validation && !IsCacheEntryValid(true, cache_entry->version_tag, cache_entry->last_modified,
	                                                    current_version_tag, current_last_modified)) {
		// Cache entry is invalid, remove it.
		cache->Delete(block_key);
		return nullptr;
	}

	return cache_entry;
}

} // namespace duckdb

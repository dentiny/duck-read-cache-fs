// Util class for disk cache operations, mainly include three parts: cache file storage, retrival and eviction, and
// cache information query.

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

#include <cstdint>
#include <functional>

namespace duckdb {

// Forward declarations.
class FileSystem;
struct InstanceConfig;

class DiskCacheUtil {
public:
	// Cache directory and cache filepath for certain request.
	struct CacheFileDestination {
		// Index for all cache directories.
		idx_t cache_directory_idx = 0;
		// Local cache filepath.
		string cache_filepath;
	};

	// Get local cache filename for the given [remote_file].
	//
	// Cache filename is formatted as `<cache-directory>/<filename-sha256>-<filename>-<start-offset>-<size>`. So we
	// could get all cache files under one directory, and get all cache files with commands like `ls`.
	//
	// Considering the naming format, it's worth noting it might _NOT_ work for local files, including mounted
	// filesystems.
	static CacheFileDestination GetLocalCacheFile(const vector<string> &cache_directories, const string &remote_file,
	                                              idx_t start_offset, idx_t bytes_to_read);

	// Remote file information for a local cache filename.
	struct RemoteFileInfo {
		string remote_filename;
		uint64_t start_offset = 0;
		uint64_t end_offset = 0;
	};

	// Get remote file information from the given local cache [fname].
	static RemoteFileInfo GetRemoteFileInfo(const string &fname);

	// Used to delete on-disk cache files, which returns the file prefix for the given [remote_file].
	static string GetLocalCacheFilePrefix(const string &remote_file);

	// Store content to a local cache file.
	// Disk space availability is validated, and eviction is triggered if needed.
	// [lru_eviction_decider] is used to obtain the filepath to remove under LRU eviction policy.
	static void StoreLocalCacheFile(const string &remote_filepath, const string &cache_directory,
	                                const string &local_cache_file, const string &content, const string &version_tag,
	                                const InstanceConfig &config, const std::function<string()> &lru_eviction_decider);

	// Result of a local cache file read attempt.
	struct LocalCacheReadResult {
		bool cache_hit = false;
		string content;
	};

	// Attempt to open, validate, and read a local cache file.
	static LocalCacheReadResult ReadLocalCacheFile(const string &cache_filepath, idx_t chunk_size, bool use_direct_io,
	                                               const string &version_tag);

private:
	// Return whether the cached file at [cache_filepath] is still valid for the given [version_tag].
	// Empty version tag means cache validation is disabled.
	static bool ValidateCacheFile(const string &cache_filepath, const string &version_tag);

	// Attempt to evict cache files, if file size threshold reached.
	// [lru_eviction_decider] is used to obtain the filepath to remove under LRU eviction policy.
	static void EvictCacheFiles(FileSystem &local_filesystem, const string &cache_directory,
	                            const string &eviction_policy, const std::function<string()> &lru_eviction_decider);
};

} // namespace duckdb

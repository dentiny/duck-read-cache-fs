#include "disk_cache_eviction_policy.hpp"

#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"
#include "utils/include/filesystem_utils.hpp"

namespace duckdb {

namespace {

// Deletes all cache files under the cache directory which haven't been accessed for a threshold.
class CreationTimestampEvictionPolicy final : public DiskCacheEvictionPolicy {
public:
	const string &GetName() const override {
		return *ON_DISK_CREATION_TIMESTAMP_EVICTION;
	}

	// Access timestamps are tracked by the filesystem, so no in-memory state to maintain.
	void OnCacheFileAccess(const string &cache_filepath) override {
	}
	void OnCacheFileStore(const string &cache_filepath) override {
	}
	void OnCacheFilesRemoved(const vector<string> &cache_filepaths) override {
	}
	void OnCacheCleared() override {
	}

	void Evict(const string &cache_directory, const InstanceConfig &config) override {
		LocalFileSystem local_filesystem {};
		EvictStaleCacheFiles(local_filesystem, cache_directory);
	}
};

// Deletes the least recently accessed cache file; access history is only tracked within the current process.
class LruSingleProcEvictionPolicy final : public DiskCacheEvictionPolicy {
public:
	const string &GetName() const override {
		return *ON_DISK_LRU_SINGLE_PROC_EVICTION;
	}

	void OnCacheFileAccess(const string &cache_filepath) override {
		UpsertAccessTimestamp(cache_filepath);
	}

	void OnCacheFileStore(const string &cache_filepath) override {
		UpsertAccessTimestamp(cache_filepath);
	}

	void OnCacheFilesRemoved(const vector<string> &cache_filepaths) override {
		const concurrency::lock_guard<concurrency::mutex> lck(mu);
		for (const auto &cur_filepath : cache_filepaths) {
			RemoveAccessTimestamp(cur_filepath);
		}
		ALWAYS_ASSERT(access_timestamp_to_filepath.size() == filepath_to_access_timestamp.size());
	}

	void OnCacheCleared() override {
		const concurrency::lock_guard<concurrency::mutex> lck(mu);
		access_timestamp_to_filepath.clear();
		filepath_to_access_timestamp.clear();
	}

	void Evict(const string &cache_directory, const InstanceConfig &config) override {
		string filepath_to_evict;
		{
			const concurrency::lock_guard<concurrency::mutex> lck(mu);
			if (access_timestamp_to_filepath.empty()) {
				LoadAccessTimestampsFromDisk(config.on_disk_cache_directories);
			}
			if (access_timestamp_to_filepath.empty()) {
				return;
			}
			filepath_to_evict = std::move(access_timestamp_to_filepath.begin()->second);
			RemoveAccessTimestamp(filepath_to_evict);
			ALWAYS_ASSERT(access_timestamp_to_filepath.size() == filepath_to_access_timestamp.size());
		}

		// Intentionally ignore return value.
		LocalFileSystem local_filesystem {};
		local_filesystem.TryRemoveFile(filepath_to_evict);
	}

private:
	// Insert or refresh [filepath] in the access maps and on-disk access time.
	void UpsertAccessTimestamp(const string &filepath) {
		UpdateFileTimestamps(filepath);

		timestamp_t ts = Timestamp::GetCurrentTimestamp();
		const concurrency::lock_guard<concurrency::mutex> lck(mu);
		RemoveAccessTimestamp(filepath);
		while (access_timestamp_to_filepath.count(ts)) {
			ts = timestamp_t {ts.value + 1};
		}
		access_timestamp_to_filepath.emplace(ts, filepath);
		const auto inserted = filepath_to_access_timestamp.emplace(filepath, ts).second;
		ALWAYS_ASSERT(inserted);
		ALWAYS_ASSERT(access_timestamp_to_filepath.size() == filepath_to_access_timestamp.size());
	}

	// Remove [filepath] from the access maps, if tracked.
	void RemoveAccessTimestamp(const string &filepath) DUCKDB_REQUIRES(mu) {
		const auto reverse_it = filepath_to_access_timestamp.find(filepath);
		if (reverse_it == filepath_to_access_timestamp.end()) {
			return;
		}
		const auto erased = access_timestamp_to_filepath.erase(reverse_it->second);
		ALWAYS_ASSERT(erased == 1);
		filepath_to_access_timestamp.erase(reverse_it);
	}

	// Rebuild access maps from cache files under [cache_directories].
	void LoadAccessTimestampsFromDisk(const vector<string> &cache_directories) DUCKDB_REQUIRES(mu) {
		ALWAYS_ASSERT(access_timestamp_to_filepath.empty());
		ALWAYS_ASSERT(filepath_to_access_timestamp.empty());

		access_timestamp_to_filepath = GetOnDiskFilesUnder(cache_directories);
		filepath_to_access_timestamp.reserve(access_timestamp_to_filepath.size());
		for (const auto &entry : access_timestamp_to_filepath) {
			const auto inserted = filepath_to_access_timestamp.emplace(entry.second, entry.first).second;
			ALWAYS_ASSERT(inserted);
		}
		ALWAYS_ASSERT(access_timestamp_to_filepath.size() == filepath_to_access_timestamp.size());
	}

	concurrency::mutex mu;
	// Maps last-access timestamp to cache filepath.
	map<timestamp_t, string> access_timestamp_to_filepath DUCKDB_GUARDED_BY(mu);
	// Maps from filepath to last-access timestamp.
	// Invariant: each filepath appears exactly once in both maps.
	unordered_map<string, timestamp_t> filepath_to_access_timestamp DUCKDB_GUARDED_BY(mu);
};

} // namespace

unique_ptr<DiskCacheEvictionPolicy> CreateDiskCacheEvictionPolicy(const string &policy_name) {
	if (policy_name == *ON_DISK_CREATION_TIMESTAMP_EVICTION) {
		return make_uniq<CreationTimestampEvictionPolicy>();
	}
	if (policy_name == *ON_DISK_LRU_SINGLE_PROC_EVICTION) {
		return make_uniq<LruSingleProcEvictionPolicy>();
	}
	throw InternalException("Unknown on-disk cache eviction policy '%s'", policy_name);
}

} // namespace duckdb

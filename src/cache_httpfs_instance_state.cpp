#include "cache_httpfs_instance_state.hpp"

#include <algorithm>

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/database.hpp"
#include "in_memory_cache_reader.hpp"
#include "noop_cache_reader.hpp"
#include "temp_profile_collector.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// InstanceCacheFsRegistry implementation
//===--------------------------------------------------------------------===//

void InstanceCacheFsRegistry::Register(CacheFileSystem *fs) {
	std::lock_guard<std::mutex> lock(mutex);
	cache_filesystems.emplace_back(fs);
}

void InstanceCacheFsRegistry::Unregister(CacheFileSystem *fs) {
	std::lock_guard<std::mutex> lock(mutex);
	auto it = std::find(cache_filesystems.begin(), cache_filesystems.end(), fs);
	if (it != cache_filesystems.end()) {
		cache_filesystems.erase(it);
	}
}

vector<CacheFileSystem *> InstanceCacheFsRegistry::GetAllCacheFs() const {
	std::lock_guard<std::mutex> lock(mutex);
	return cache_filesystems;
}

void InstanceCacheFsRegistry::Reset() {
	std::lock_guard<std::mutex> lock(mutex);
	cache_filesystems.clear();
}

//===--------------------------------------------------------------------===//
// InstanceProfileCollectorManager implementation
//===--------------------------------------------------------------------===//

void InstanceProfileCollectorManager::SetProfileCollector(connection_t connection_id, const string &profile_type) {
	std::lock_guard<std::mutex> lock(mutex);

	auto it = profile_collectors.find(connection_id);

	// Check if we already have the right type for this connection
	if (it != profile_collectors.end() && it->second != nullptr && it->second->GetProfilerType() == profile_type) {
		return; // Already have the right type
	}

	if (profile_type == *NOOP_PROFILE_TYPE) {
		profile_collectors[connection_id] = make_uniq<NoopProfileCollector>();
		return;
	}

	if (profile_type == *TEMP_PROFILE_TYPE) {
		profile_collectors[connection_id] = make_uniq<TempProfileCollector>();
		return;
	}

	// Default to noop if unknown type and no collector exists for this connection
	if (it == profile_collectors.end() || it->second == nullptr) {
		profile_collectors[connection_id] = make_uniq<NoopProfileCollector>();
	}
}

BaseProfileCollector *InstanceProfileCollectorManager::GetProfileCollector(connection_t connection_id) const {
	std::lock_guard<std::mutex> lock(mutex);
	auto it = profile_collectors.find(connection_id);
	if (it != profile_collectors.end()) {
		return it->second.get();
	}
	return nullptr;
}

void InstanceProfileCollectorManager::ResetProfileCollector(connection_t connection_id) {
	std::lock_guard<std::mutex> lock(mutex);
	auto it = profile_collectors.find(connection_id);
	if (it != profile_collectors.end() && it->second != nullptr) {
		it->second->Reset();
	}
}

//===--------------------------------------------------------------------===//
// InstanceCacheReaderManager implementation
//===--------------------------------------------------------------------===//

void InstanceCacheReaderManager::SetCacheReader(const InstanceConfig &config,
                                                weak_ptr<CacheHttpfsInstanceState> instance_state_p) {
	std::lock_guard<std::mutex> lock(mutex);

	if (config.cache_type == *NOOP_CACHE_TYPE) {
		if (noop_cache_reader == nullptr) {
			noop_cache_reader = make_uniq<NoopCacheReader>();
		}
		internal_cache_reader = noop_cache_reader.get();
		return;
	}

	if (config.cache_type == *ON_DISK_CACHE_TYPE) {
		if (on_disk_cache_reader == nullptr) {
			on_disk_cache_reader =
			    make_uniq<DiskCacheReader>(std::move(instance_state_p), config.on_disk_cache_directories);
		}
		internal_cache_reader = on_disk_cache_reader.get();
		return;
	}

	if (config.cache_type == *IN_MEM_CACHE_TYPE) {
		if (in_mem_cache_reader == nullptr) {
			in_mem_cache_reader = make_uniq<InMemoryCacheReader>(std::move(instance_state_p));
		}
		internal_cache_reader = in_mem_cache_reader.get();
		return;
	}
}

BaseCacheReader *InstanceCacheReaderManager::GetCacheReader() const {
	std::lock_guard<std::mutex> lock(mutex);
	return internal_cache_reader;
}

vector<BaseCacheReader *> InstanceCacheReaderManager::GetCacheReaders() const {
	std::lock_guard<std::mutex> lock(mutex);
	vector<BaseCacheReader *> result;
	if (in_mem_cache_reader != nullptr) {
		result.emplace_back(in_mem_cache_reader.get());
	}
	if (on_disk_cache_reader != nullptr) {
		result.emplace_back(on_disk_cache_reader.get());
	}
	return result;
}

void InstanceCacheReaderManager::InitializeDiskCacheReader(const vector<string> &cache_directories,
                                                           weak_ptr<CacheHttpfsInstanceState> instance_state_p) {
	std::lock_guard<std::mutex> lock(mutex);
	if (on_disk_cache_reader == nullptr) {
		on_disk_cache_reader = make_uniq<DiskCacheReader>(std::move(instance_state_p), cache_directories);
	}
}

void InstanceCacheReaderManager::ClearCache() {
	std::lock_guard<std::mutex> lock(mutex);
	if (noop_cache_reader != nullptr) {
		noop_cache_reader->ClearCache();
	}
	if (in_mem_cache_reader != nullptr) {
		in_mem_cache_reader->ClearCache();
	}
	if (on_disk_cache_reader != nullptr) {
		on_disk_cache_reader->ClearCache();
	}
}

void InstanceCacheReaderManager::ClearCache(const string &fname) {
	std::lock_guard<std::mutex> lock(mutex);
	if (noop_cache_reader != nullptr) {
		noop_cache_reader->ClearCache(fname);
	}
	if (in_mem_cache_reader != nullptr) {
		in_mem_cache_reader->ClearCache(fname);
	}
	if (on_disk_cache_reader != nullptr) {
		on_disk_cache_reader->ClearCache(fname);
	}
}

void InstanceCacheReaderManager::Reset() {
	std::lock_guard<std::mutex> lock(mutex);
	noop_cache_reader.reset();
	in_mem_cache_reader.reset();
	on_disk_cache_reader.reset();
	internal_cache_reader = nullptr;
}

//===--------------------------------------------------------------------===//
// InstanceConfig implementation
//===--------------------------------------------------------------------===//

void InstanceConfig::UpdateFromOpener(optional_ptr<FileOpener> opener) {
	if (opener == nullptr) {
		// Apply test_cache_type override if set
		if (!test_cache_type.empty()) {
			cache_type = test_cache_type;
		}
		// Ensure cache directories exist
		auto local_fs = LocalFileSystem::CreateLocal();
		for (const auto &dir : on_disk_cache_directories) {
			local_fs->CreateDirectory(dir);
		}
		return;
	}

	Value val;

	//===--------------------------------------------------------------------===//
	// Global cache configuration
	//===--------------------------------------------------------------------===//

	// Cache type - only update if explicitly set to non-default
	// This prevents one connection with default settings from overwriting another's explicit config
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_type", val);
	auto cache_type_str = val.ToString();
	if (ALL_CACHE_TYPES.find(cache_type_str) != ALL_CACHE_TYPES.end() && cache_type_str != *DEFAULT_CACHE_TYPE) {
		cache_type = std::move(cache_type_str);
	}

	// Test cache type override
	if (!test_cache_type.empty()) {
		cache_type = test_cache_type;
	}

	// Block size - only update if explicitly set to non-default
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_cache_block_size", val);
	const auto cache_block_size = val.GetValue<uint64_t>();
	if (cache_block_size > 0 && cache_block_size != DEFAULT_CACHE_BLOCK_SIZE) {
		this->cache_block_size = cache_block_size;
	}

	// Profile type - only update if explicitly set to non-default
	// This prevents one connection with default settings from overwriting another's explicit config
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_profile_type", val);
	auto profile_type_str = val.ToString();
	if (ALL_PROFILE_TYPES->find(profile_type_str) != ALL_PROFILE_TYPES->end() &&
	    profile_type_str != *DEFAULT_PROFILE_TYPE) {
		profile_type = std::move(profile_type_str);
	}

	// Check and update configuration for max subrequest count.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_max_fanout_subrequest", val);
	max_subrequest_count = val.GetValue<uint64_t>();

	//===--------------------------------------------------------------------===//
	// On-disk cache configuration
	//===--------------------------------------------------------------------===//

	if (cache_type == *ON_DISK_CACHE_TYPE) {
		// Check and update cache directory if necessary.
		//
		// TODO(hjiang): Parse cache directory might be expensive, consider adding a cache besides.
		auto new_on_disk_cache_directories = GetCacheDirectoryConfig(opener);
		D_ASSERT(!new_on_disk_cache_directories.empty());
		if (new_on_disk_cache_directories != on_disk_cache_directories) {
			for (const auto &cur_cache_dir : new_on_disk_cache_directories) {
				LocalFileSystem::CreateLocal()->CreateDirectory(cur_cache_dir);
			}
			on_disk_cache_directories = std::move(new_on_disk_cache_directories);
		}

		// Check and update min bytes for disk cache.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_min_disk_bytes_for_cache", val);
		const auto disk_cache_min_bytes = val.GetValue<uint64_t>();
		if (disk_cache_min_bytes > 0) {
			min_disk_bytes_for_cache = disk_cache_min_bytes;
		}

		// Check and update eviction policy.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_evict_policy", val);
		const auto eviction_policy = val.ToString();
		if (eviction_policy == *ON_DISK_CREATION_TIMESTAMP_EVICTION) {
			on_disk_eviction_policy = *ON_DISK_CREATION_TIMESTAMP_EVICTION;
		} else if (eviction_policy == *ON_DISK_LRU_SINGLE_PROC_EVICTION) {
			on_disk_eviction_policy = *ON_DISK_LRU_SINGLE_PROC_EVICTION;
		}

		// Update disk cache reader memory cache configs.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_disk_cache_reader_enable_memory_cache", val);
		enable_disk_reader_mem_cache = val.GetValue<bool>();

		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_disk_cache_reader_mem_cache_block_count", val);
		disk_reader_max_mem_cache_block_count = val.GetValue<idx_t>();

		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_disk_cache_reader_mem_cache_timeout_millisec", val);
		disk_reader_max_mem_cache_timeout_millisec = val.GetValue<idx_t>();
	}

	//===--------------------------------------------------------------------===//
	// In-mem cache configuration
	//===--------------------------------------------------------------------===//

	// Check and update configurations for in-memory cache type.
	if (cache_type == *IN_MEM_CACHE_TYPE) {
		// Check and update max cache block count.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_max_in_mem_cache_block_count", val);
		const auto in_mem_block_count = val.GetValue<uint64_t>();
		if (in_mem_block_count > 0) {
			max_in_mem_cache_block_count = in_mem_block_count;
		}

		// Check and update in-memory data block caxche timeout.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_in_mem_cache_block_timeout_millisec", val);
		in_mem_cache_block_timeout_millisec = val.GetValue<uint64_t>();
	}

	//===--------------------------------------------------------------------===//
	// Metadata cache configuration
	//===--------------------------------------------------------------------===//

	// Check and update configurations for metadata cache enablement.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_enable_metadata_cache", val);
	enable_metadata_cache = val.GetValue<bool>();

	// Check and update metadata cache config if enabled.
	if (enable_metadata_cache) {
		// Check and update cache entry size.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_metadata_cache_entry_size", val);
		max_metadata_cache_entry = val.GetValue<uint64_t>();

		// Check and update cache entry timeout.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_metadata_cache_entry_timeout_millisec", val);
		metadata_cache_entry_timeout_millisec = val.GetValue<uint64_t>();
	}

	//===--------------------------------------------------------------------===//
	// File handle cache configuration
	//===--------------------------------------------------------------------===//

	// Check and update configurations for file handle cache enablement.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_enable_file_handle_cache", val);
	enable_file_handle_cache = val.GetValue<bool>();

	// Check and update file handle cache config if enabled.
	if (enable_file_handle_cache) {
		// Check and update cache entry size.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_file_handle_cache_entry_size", val);
		max_file_handle_cache_entry = val.GetValue<uint64_t>();

		// Check and update cache entry timeout.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_file_handle_cache_entry_timeout_millisec", val);
		file_handle_cache_entry_timeout_millisec = val.GetValue<uint64_t>();
	}

	//===--------------------------------------------------------------------===//
	// Glob cache configuration
	//===--------------------------------------------------------------------===//

	// Check and update configurations for glob cache enablement.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_enable_glob_cache", val);
	enable_glob_cache = val.GetValue<bool>();

	// Check and update file handle cache config if enabled.
	if (enable_glob_cache) {
		// Check and update cache entry size.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_glob_cache_entry_size", val);
		max_glob_cache_entry = val.GetValue<uint64_t>();

		// Check and update cache entry timeout.
		FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_glob_cache_entry_timeout_millisec", val);
		glob_cache_entry_timeout_millisec = val.GetValue<uint64_t>();
	}

	//===--------------------------------------------------------------------===//
	// Cache validation configuration
	//===--------------------------------------------------------------------===//

	// Check and update cache validation enablement.
	FileOpener::TryGetCurrentSetting(opener, "cache_httpfs_enable_cache_validation", val);
	enable_cache_validation = val.GetValue<bool>();
}

//===--------------------------------------------------------------------===//
// Instance state storage/retrieval using DuckDB's ObjectCache
//===--------------------------------------------------------------------===//

void SetInstanceState(DatabaseInstance &instance, shared_ptr<CacheHttpfsInstanceState> state) {
	instance.GetObjectCache().Put(CacheHttpfsInstanceState::CACHE_KEY, std::move(state));
}

CacheHttpfsInstanceState *GetInstanceState(DatabaseInstance &instance) {
	auto state = instance.GetObjectCache().Get<CacheHttpfsInstanceState>(CacheHttpfsInstanceState::CACHE_KEY);
	return state.get();
}

shared_ptr<CacheHttpfsInstanceState> GetInstanceStateShared(DatabaseInstance &instance) {
	return instance.GetObjectCache().Get<CacheHttpfsInstanceState>(CacheHttpfsInstanceState::CACHE_KEY);
}

CacheHttpfsInstanceState &GetInstanceStateOrThrow(DatabaseInstance &instance) {
	auto *state = GetInstanceState(instance);
	if (state == nullptr) {
		throw InternalException("cache_httpfs instance state not found - extension not properly loaded");
	}
	return *state;
}

} // namespace duckdb

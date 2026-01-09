#include "cache_httpfs_instance_state.hpp"

#include "assert_utils.hpp"
#include "base_profile_collector.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "in_memory_cache_reader.hpp"
#include "noop_cache_reader.hpp"
#include "noop_profile_collector.hpp"
#include "temp_profile_collector.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// InstanceCacheFsRegistry implementation
//===--------------------------------------------------------------------===//

void InstanceCacheFsRegistry::Register(CacheFileSystem *fs) {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	cache_filesystems.insert(fs);
}

void InstanceCacheFsRegistry::Unregister(CacheFileSystem *fs) {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	cache_filesystems.erase(fs);
}

unordered_set<CacheFileSystem *> InstanceCacheFsRegistry::GetAllCacheFs() const {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	return cache_filesystems;
}

void InstanceCacheFsRegistry::Reset() {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	cache_filesystems.clear();
}

//===--------------------------------------------------------------------===//
// InstanceCacheReaderManager implementation
//===--------------------------------------------------------------------===//

void InstanceCacheReaderManager::SetCacheReader(const InstanceConfig &config,
                                                weak_ptr<CacheHttpfsInstanceState> instance_state_p) {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	auto instance_state_locked = instance_state_p.lock();
	if (!instance_state_locked) {
		throw InternalException("Instance state is no longer valid when setting cache reader");
	}

	if (config.cache_type == *ON_DISK_CACHE_TYPE) {
		if (on_disk_cache_reader == nullptr) {
			on_disk_cache_reader =
			    make_uniq<DiskCacheReader>(std::move(instance_state_p), *instance_state_locked->profile_collector);
		} else {
			on_disk_cache_reader->SetProfileCollector(*instance_state_locked->profile_collector);
		}
		internal_cache_reader = on_disk_cache_reader.get();
		return;
	}

	if (config.cache_type == *IN_MEM_CACHE_TYPE) {
		if (in_mem_cache_reader == nullptr) {
			in_mem_cache_reader =
			    make_uniq<InMemoryCacheReader>(std::move(instance_state_p), *instance_state_locked->profile_collector);
		} else {
			in_mem_cache_reader->SetProfileCollector(*instance_state_locked->profile_collector);
		}
		internal_cache_reader = in_mem_cache_reader.get();
		return;
	}

	// Fallback to NoopCacheReader.
	if (noop_cache_reader == nullptr) {
		noop_cache_reader = make_uniq<NoopCacheReader>(*instance_state_locked->profile_collector);
	} else {
		noop_cache_reader->SetProfileCollector(*instance_state_locked->profile_collector);
	}

	internal_cache_reader = noop_cache_reader.get();
}

BaseCacheReader *InstanceCacheReaderManager::GetCacheReader() const {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	return internal_cache_reader;
}

vector<BaseCacheReader *> InstanceCacheReaderManager::GetCacheReaders() const {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
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
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);

	auto instance_state_locked = instance_state_p.lock();
	if (!instance_state_locked) {
		throw InternalException("Instance state is no longer valid when initializing disk cache reader");
	}

	if (on_disk_cache_reader == nullptr) {
		on_disk_cache_reader =
		    make_uniq<DiskCacheReader>(std::move(instance_state_p), *instance_state_locked->profile_collector);
	} else {
		on_disk_cache_reader->SetProfileCollector(*instance_state_locked->profile_collector);
	}
}

void InstanceCacheReaderManager::UpdateProfileCollector(BaseProfileCollector &profile_collector) {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	if (noop_cache_reader != nullptr) {
		noop_cache_reader->SetProfileCollector(profile_collector);
	}
	if (in_mem_cache_reader != nullptr) {
		in_mem_cache_reader->SetProfileCollector(profile_collector);
	}
	if (on_disk_cache_reader != nullptr) {
		on_disk_cache_reader->SetProfileCollector(profile_collector);
	}
}

void InstanceCacheReaderManager::ClearCache() {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
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
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
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
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);
	noop_cache_reader.reset();
	in_mem_cache_reader.reset();
	on_disk_cache_reader.reset();
	internal_cache_reader = nullptr;
}

//===--------------------------------------------------------------------===//
// Instance state storage/retrieval using DuckDB's ObjectCache
//===--------------------------------------------------------------------===//

void SetInstanceState(DatabaseInstance &instance, shared_ptr<CacheHttpfsInstanceState> state) {
	instance.GetObjectCache().Put(CacheHttpfsInstanceState::CACHE_KEY, std::move(state));
}

shared_ptr<CacheHttpfsInstanceState> GetInstanceStateShared(DatabaseInstance &instance) {
	return instance.GetObjectCache().Get<CacheHttpfsInstanceState>(CacheHttpfsInstanceState::CACHE_KEY);
}

CacheHttpfsInstanceState &GetInstanceStateOrThrow(DatabaseInstance &instance) {
	auto state = instance.GetObjectCache().Get<CacheHttpfsInstanceState>(CacheHttpfsInstanceState::CACHE_KEY);
	if (state == nullptr) {
		throw InternalException("cache_httpfs instance state not found - extension not properly loaded");
	}
	return *state;
}

CacheHttpfsInstanceState &GetInstanceStateOrThrow(ClientContext &context) {
	return GetInstanceStateOrThrow(*context.db.get());
}

shared_ptr<CacheHttpfsInstanceState> GetInstanceConfig(weak_ptr<CacheHttpfsInstanceState> instance_state) {
	auto instance_state_locked = instance_state.lock();
	if (instance_state_locked == nullptr) {
		throw InternalException("cache_httpfs instance state is no longer valid");
	}
	return instance_state_locked;
}

//===--------------------------------------------------------------------===//
// CacheHttpfsInstanceState implementation
//===--------------------------------------------------------------------===//

CacheHttpfsInstanceState::CacheHttpfsInstanceState() : profile_collector(make_uniq<NoopProfileCollector>()) {
}

BaseProfileCollector &CacheHttpfsInstanceState::GetProfileCollector() {
	CACHE_HTTPFS_ALWAYS_ASSERT(profile_collector != nullptr);
	return *profile_collector;
}

void CacheHttpfsInstanceState::ResetProfileCollector() {
	SetProfileCollector(*NOOP_PROFILE_TYPE);
}

void CacheHttpfsInstanceState::SetProfileCollector(string profile_type) {
	if (ALL_PROFILE_TYPES->find(profile_type) == ALL_PROFILE_TYPES->end()) {
		const vector<string> valid_types(ALL_PROFILE_TYPES->begin(), ALL_PROFILE_TYPES->end());
		throw InvalidInputException("Invalid cache_httpfs_profile_type '%s'. Valid options are: %s", profile_type,
		                            StringUtil::Join(valid_types, ", "));
	}
	config.profile_type = std::move(profile_type);

	// Initialize the profile collector based on the new profile type
	CACHE_HTTPFS_ALWAYS_ASSERT(profile_collector != nullptr);
	if (profile_collector->GetProfilerType() == config.profile_type) {
		return;
	}

	// Update the profile collector.
	if (config.profile_type == *NOOP_PROFILE_TYPE) {
		profile_collector = make_uniq<NoopProfileCollector>();
	} else if (config.profile_type == *TEMP_PROFILE_TYPE) {
		profile_collector = make_uniq<TempProfileCollector>();
	} else {
		profile_collector = make_uniq<NoopProfileCollector>();
	}

	// Update all cache readers to use the new collector
	cache_reader_manager.UpdateProfileCollector(*profile_collector);

	// Update all registered cache filesystems to use the new profile collector
	auto cache_filesystems = registry.GetAllCacheFs();
	for (auto *cache_fs : cache_filesystems) {
		cache_fs->SetProfileCollector(*profile_collector);
	}
}

} // namespace duckdb

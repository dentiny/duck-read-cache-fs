#include "cache_httpfs_instance_state.hpp"

#include "base_profile_collector.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
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
// InstanceProfileCollectorManager implementation
//===--------------------------------------------------------------------===//

void InstanceProfileCollectorManager::SetProfileCollector(connection_t connection_id, const string &profile_type) {
	concurrency::lock_guard<concurrency::mutex> lock(mutex);

	auto it = profile_collectors.find(connection_id);

	if (it != profile_collectors.end() && it->second != nullptr && it->second->GetProfilerType() == profile_type) {
		return;
	}

	if (profile_type == *NOOP_PROFILE_TYPE) {
		profile_collectors[connection_id] = make_uniq<NoopProfileCollector>();
		return;
	}

	if (profile_type == *TEMP_PROFILE_TYPE) {
		profile_collectors[connection_id] = make_uniq<TempProfileCollector>();
		return;
	}

	// PERSISTENT_PROFILE_TYPE ("duckdb") is declared but not yet implemented;
	// fall back to noop so the setting is accepted without error.
	profile_collectors[connection_id] = make_uniq<NoopProfileCollector>();
}

BaseProfileCollector *InstanceProfileCollectorManager::GetProfileCollector(connection_t connection_id) const {
	concurrency::lock_guard<concurrency::mutex> lock(mutex);
	auto it = profile_collectors.find(connection_id);
	if (it != profile_collectors.end()) {
		return it->second.get();
	}
	return nullptr;
}

void InstanceProfileCollectorManager::ResetProfileCollector(connection_t connection_id) {
	concurrency::lock_guard<concurrency::mutex> lock(mutex);
	auto it = profile_collectors.find(connection_id);
	if (it != profile_collectors.end() && it->second != nullptr) {
		it->second->Reset();
	}
}

void InstanceProfileCollectorManager::RemoveProfileCollector(connection_t connection_id) {
	concurrency::lock_guard<concurrency::mutex> lock(mutex);
	profile_collectors.erase(connection_id);
}

//===--------------------------------------------------------------------===//
// InstanceCacheReaderManager implementation
//===--------------------------------------------------------------------===//

void InstanceCacheReaderManager::SetCacheReader(const InstanceConfig &config,
                                                weak_ptr<CacheHttpfsInstanceState> instance_state_p) {
	const concurrency::lock_guard<concurrency::mutex> lock(mutex);

	if (config.cache_type == *ON_DISK_CACHE_TYPE) {
		if (on_disk_cache_reader == nullptr) {
			on_disk_cache_reader = make_uniq<DiskCacheReader>(std::move(instance_state_p));
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

	// Fallback to NoopCacheReader.
	if (noop_cache_reader == nullptr) {
		noop_cache_reader = make_uniq<NoopCacheReader>(std::move(instance_state_p));
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
	if (on_disk_cache_reader == nullptr) {
		on_disk_cache_reader = make_uniq<DiskCacheReader>(std::move(instance_state_p));
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

shared_ptr<CacheHttpfsInstanceState> GetInstanceConfig(const weak_ptr<CacheHttpfsInstanceState> &instance_state) {
	auto instance_state_locked = instance_state.lock();
	if (instance_state_locked == nullptr) {
		throw InternalException("cache_httpfs instance state is no longer valid");
	}
	return instance_state_locked;
}

BaseProfileCollector &GetProfileCollectorOrThrow(const shared_ptr<CacheHttpfsInstanceState> &instance_state,
                                                 connection_t conn_id) {
	D_ASSERT(instance_state);
	auto *collector = instance_state->profile_collector_manager.GetProfileCollector(conn_id);
	if (!collector) {
		throw InternalException("CacheFileSystem: no profile collector for connection %llu", conn_id);
	}
	return *collector;
}

//===--------------------------------------------------------------------===//
// Per-connection cleanup via ClientContextState
//===--------------------------------------------------------------------===//

namespace {

constexpr const char *CONNECTION_STATE_KEY = "cache_httpfs_connection_state";

class CacheHttpfsConnectionState : public ClientContextState {
public:
	CacheHttpfsConnectionState(weak_ptr<CacheHttpfsInstanceState> instance_state_p, connection_t connection_id_p)
	    : instance_state(std::move(instance_state_p)), connection_id(connection_id_p) {
	}

	~CacheHttpfsConnectionState() override {
		auto state = instance_state.lock();
		if (state) {
			state->profile_collector_manager.RemoveProfileCollector(connection_id);
		}
	}

private:
	weak_ptr<CacheHttpfsInstanceState> instance_state;
	connection_t connection_id;
};

} // namespace

void RegisterConnectionCleanupState(ClientContext &context, weak_ptr<CacheHttpfsInstanceState> instance_state) {
	context.registered_state->GetOrCreate<CacheHttpfsConnectionState>(CONNECTION_STATE_KEY, std::move(instance_state),
	                                                                  context.GetConnectionId());
}

} // namespace duckdb

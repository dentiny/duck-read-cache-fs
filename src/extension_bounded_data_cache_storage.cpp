#include "extension_bounded_data_cache_storage.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include "time_utils.hpp"

namespace duckdb {

namespace {

uint64_t GetNowMilliSec() {
	return static_cast<uint64_t>(GetSteadyNowMilliSecSinceEpoch());
}

} // namespace

ExtensionBoundedDataCacheStorage::ExtensionBoundedDataCacheStorage(
    unique_ptr<CacheEvictionPolicy<InMemCacheBlock>> eviction_policy_p, uint64_t timeout_millisec_p)
    : timeout_millisec(timeout_millisec_p), eviction_policy(std::move(eviction_policy_p)) {
	ALWAYS_ASSERT(eviction_policy != nullptr);
}

bool ExtensionBoundedDataCacheStorage::IsExpired(const Entry &entry, uint64_t now) const {
	return timeout_millisec > 0 && now - entry.timestamp > timeout_millisec;
}

void ExtensionBoundedDataCacheStorage::DeleteImpl(EntryMap::iterator iter) {
	eviction_policy->OnRemove(iter->first);
	entries.erase(iter);
}

void ExtensionBoundedDataCacheStorage::Put(InMemCacheBlock key, PageAlignedDataChunk chunk, string version_tag) {
	auto value = make_shared_ptr<InMemCacheDataEntry>();
	value->data = std::move(chunk);
	value->version_tag = std::move(version_tag);
	Entry new_entry {
	    .value = std::move(value),
	    .timestamp = GetNowMilliSec(),
	};

	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	auto iter = entries.find(key);
	if (iter != entries.end()) {
		iter->second = std::move(new_entry);
		eviction_policy->OnAccess(iter->first);
		return;
	}

	auto evicted_keys = eviction_policy->OnInsert(key);
	entries.emplace(std::move(key), std::move(new_entry));
	for (const auto &cur_key : evicted_keys) {
		const auto erased = entries.erase(cur_key);
		ALWAYS_ASSERT(erased == 1);
	}
}

optional<PinnedBlock> ExtensionBoundedDataCacheStorage::Get(const InMemCacheBlock &key,
                                                            const string &expected_version_tag) {
	shared_ptr<InMemCacheDataEntry> value;
	{
		const concurrency::lock_guard<concurrency::mutex> lck(mu);
		auto iter = entries.find(key);
		if (iter == entries.end()) {
			return nullopt;
		}
		if (IsExpired(iter->second, GetNowMilliSec()) ||
		    !PinnedBlock::ValidateVersionTag(iter->second.value->version_tag, expected_version_tag)) {
			DeleteImpl(iter);
			return nullopt;
		}
		eviction_policy->OnAccess(iter->first);
		value = iter->second.value;
	}

	const PageAlignedDataChunk *chunk_ptr = &value->data;
	shared_ptr<void> keep_alive = std::move(value);
	return PinnedBlock {std::move(keep_alive), chunk_ptr};
}

bool ExtensionBoundedDataCacheStorage::Delete(const InMemCacheBlock &key) {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	auto iter = entries.find(key);
	if (iter == entries.end()) {
		return false;
	}
	DeleteImpl(iter);
	return true;
}

void ExtensionBoundedDataCacheStorage::Clear() {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	entries.clear();
	eviction_policy->Clear();
}

void ExtensionBoundedDataCacheStorage::Clear(const InMemCacheBlock &start_key,
                                             std::function<bool(const InMemCacheBlock &)> filter) {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	auto iter = entries.lower_bound(start_key);
	while (iter != entries.end() && filter(iter->first)) {
		eviction_policy->OnRemove(iter->first);
		iter = entries.erase(iter);
	}
}

vector<InMemCacheBlock> ExtensionBoundedDataCacheStorage::Keys() const {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	vector<InMemCacheBlock> keys;
	keys.reserve(entries.size());
	for (const auto &cur_entry : entries) {
		keys.emplace_back(cur_entry.first);
	}
	return keys;
}

vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> ExtensionBoundedDataCacheStorage::Take() {
	const concurrency::lock_guard<concurrency::mutex> lck(mu);
	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> result;
	result.reserve(entries.size());
	const auto now = GetNowMilliSec();
	for (auto &cur_entry : entries) {
		if (IsExpired(cur_entry.second, now)) {
			continue;
		}
		result.emplace_back(cur_entry.first, std::move(cur_entry.second.value));
	}
	entries.clear();
	eviction_policy->Clear();
	return result;
}

} // namespace duckdb

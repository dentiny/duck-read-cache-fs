#include "extension_bounded_data_cache_storage.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

ExtensionBoundedDataCacheStorage::ExtensionBoundedDataCacheStorage(size_t max_entries, uint64_t timeout_millisec)
    : lru_cache(max_entries, timeout_millisec) {
}

void ExtensionBoundedDataCacheStorage::Put(InMemCacheBlock key, PageAlignedDataChunk chunk, string version_tag) {
	auto entry = make_shared_ptr<InMemCacheDataEntry>();
	entry->data = std::move(chunk);
	entry->version_tag = std::move(version_tag);
	lru_cache.Put(std::move(key), std::move(entry));
}

unique_ptr<PinnedBlock> ExtensionBoundedDataCacheStorage::Get(const InMemCacheBlock &key) {
	auto entry = lru_cache.Get(key);
	if (entry == nullptr) {
		return nullptr;
	}
	// The shared_ptr<InMemCacheDataEntry> kept inside PinnedBlock pins the chunk for the read's duration.
	const PageAlignedDataChunk *chunk_ptr = &entry->data;
	string version_tag = entry->version_tag;
	shared_ptr<void> keep_alive = std::move(entry);
	return make_uniq<PinnedBlock>(std::move(keep_alive), chunk_ptr, std::move(version_tag));
}

bool ExtensionBoundedDataCacheStorage::Delete(const InMemCacheBlock &key) {
	return lru_cache.Delete(key);
}

void ExtensionBoundedDataCacheStorage::Clear() {
	lru_cache.Clear();
}

void ExtensionBoundedDataCacheStorage::Clear(const InMemCacheBlock &start_key,
                                             std::function<bool(const InMemCacheBlock &)> filter) {
	lru_cache.Clear(start_key, std::move(filter));
}

vector<InMemCacheBlock> ExtensionBoundedDataCacheStorage::Keys() const {
	return lru_cache.Keys();
}

vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> ExtensionBoundedDataCacheStorage::Take() {
	return lru_cache.Take();
}

} // namespace duckdb

#include "extension_bounded_data_cache_storage.hpp"

namespace duckdb {

ExtensionBoundedDataCacheStorage::ExtensionBoundedDataCacheStorage(size_t max_entries, uint64_t timeout_millisec)
    : lru_cache(max_entries, timeout_millisec) {
}

void ExtensionBoundedDataCacheStorage::Put(InMemCacheBlock key, shared_ptr<InMemCacheDataEntry> value) {
	lru_cache.Put(std::move(key), std::move(value));
}

shared_ptr<InMemCacheDataEntry> ExtensionBoundedDataCacheStorage::Get(const InMemCacheBlock &key) {
	return lru_cache.Get(key);
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

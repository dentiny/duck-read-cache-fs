// LRU-based implementation of in-memory data cache manager.

#pragma once

#include "in_memory_data_cache_manager.hpp"
#include "shared_value_lru_cache.hpp"

namespace duckdb {

template <typename Key, typename Val, typename KeyCompare = std::less<Key>>
class LruDataCacheManager : public InMemoryDataCacheManager<Key, Val, KeyCompare> {
public:
	// @param max_entries: Maximum number of entries in the cache. 0 means no limit.
	// @param timeout_millisec: Timeout in milliseconds for cache entries. 0 means no timeout.
	LruDataCacheManager(size_t max_entries, uint64_t timeout_millisec) : lru_cache(max_entries, timeout_millisec) {
	}

	// Disable copy and move.
	LruDataCacheManager(const LruDataCacheManager &) = delete;
	LruDataCacheManager &operator=(const LruDataCacheManager &) = delete;

	~LruDataCacheManager() override = default;

	void Put(Key key, shared_ptr<Val> value) override {
		lru_cache.Put(std::move(key), std::move(value));
	}

	shared_ptr<Val> Get(const Key &key) override {
		return lru_cache.Get(key);
	}

	bool Delete(const Key &key) override {
		return lru_cache.Delete(key);
	}

	void Clear() override {
		lru_cache.Clear();
	}

	vector<Key> Keys() const override {
		return lru_cache.Keys();
	}

protected:
	void ClearWithFilter(const Key &start_key, std::function<bool(const Key &)> key_filter) override {
		lru_cache.Clear(start_key, std::move(key_filter));
	}

private:
	ThreadSafeSharedValueLruCache<Key, Val, KeyCompare> lru_cache;
};

} // namespace duckdb

// Least recently used eviction policy.

#pragma once

#include <functional>
#include <list>

#include "cache_eviction_policy.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/vector.hpp"
#include "map_utils.hpp"

namespace duckdb {

template <typename Key, typename KeyCompare = std::less<Key>>
class LruEvictionPolicy final : public CacheEvictionPolicy<Key> {
public:
	// @param max_entries_p: 0 means unbounded.
	explicit LruEvictionPolicy(size_t max_entries_p) : max_entries(max_entries_p) {
	}

	LruEvictionPolicy(const LruEvictionPolicy &) = delete;
	LruEvictionPolicy &operator=(const LruEvictionPolicy &) = delete;

	vector<Key> OnInsert(const Key &key) override {
		lru_list.emplace_front(key);
		const auto inserted = key_to_iter.emplace(std::cref(lru_list.front()), lru_list.begin()).second;
		ALWAYS_ASSERT(inserted);

		vector<Key> evicted;
		if (max_entries > 0 && lru_list.size() > max_entries) {
			key_to_iter.erase(lru_list.back());
			evicted.emplace_back(std::move(lru_list.back()));
			lru_list.pop_back();
		}
		return evicted;
	}

	void OnAccess(const Key &key) override {
		auto iter = key_to_iter.find(key);
		ALWAYS_ASSERT(iter != key_to_iter.end());
		lru_list.splice(lru_list.begin(), lru_list, iter->second);
	}

	void OnRemove(const Key &key) override {
		auto iter = key_to_iter.find(key);
		ALWAYS_ASSERT(iter != key_to_iter.end());
		auto list_iter = iter->second;
		key_to_iter.erase(iter);
		lru_list.erase(list_iter);
	}

	void Clear() override {
		key_to_iter.clear();
		lru_list.clear();
	}

private:
	using KeyConstRef = std::reference_wrapper<const Key>;

	const size_t max_entries;
	// The front of the list identifies the most recently accessed key; keys in [key_to_iter] are owned by the list.
	std::list<Key> lru_list;
	map<KeyConstRef, typename std::list<Key>::iterator, RefLess<KeyCompare>> key_to_iter;
};

} // namespace duckdb

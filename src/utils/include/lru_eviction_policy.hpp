// Least recently used eviction policy.

#pragma once

#include <functional>
#include <list>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/map.hpp"
#include "eviction_policy.hpp"
#include "map_utils.hpp"

namespace duckdb {

template <typename Key, typename KeyCompare = std::less<Key>>
class LruEvictionPolicy final : public EvictionPolicy<Key> {
public:
	LruEvictionPolicy() = default;

	LruEvictionPolicy(const LruEvictionPolicy &) = delete;
	LruEvictionPolicy &operator=(const LruEvictionPolicy &) = delete;

	void OnInsert(const Key &key) override {
		lru_list.emplace_front(key);
		const auto inserted = key_to_iter.emplace(std::cref(lru_list.front()), lru_list.begin()).second;
		ALWAYS_ASSERT(inserted);
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

	bool Contains(const Key &key) const override {
		return key_to_iter.find(key) != key_to_iter.end();
	}

	optional<Key> PopVictim() override {
		if (lru_list.empty()) {
			return nullopt;
		}
		key_to_iter.erase(lru_list.back());
		Key victim = std::move(lru_list.back());
		lru_list.pop_back();
		return victim;
	}

	void Clear() override {
		key_to_iter.clear();
		lru_list.clear();
	}

private:
	using KeyConstRef = std::reference_wrapper<const Key>;

	// The front of the list identifies the most recently accessed key; keys in [key_to_iter] are owned by the list.
	std::list<Key> lru_list;
	map<KeyConstRef, typename std::list<Key>::iterator, RefLess<KeyCompare>> key_to_iter;
};

} // namespace duckdb

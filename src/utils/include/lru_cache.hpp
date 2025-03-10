// SharedLruCache is a LRU cache, with all entries shared, which allows each key value pair could be read from multiple
// requests.
//
// It's made for values which are expensive to copy, so we use shared pointer wrapper for all values.

#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <unordered_map>
#include <string>
#include <utility>
#include <type_traits>
#include <mutex>

#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
#include "map_utils.hpp"

namespace duckdb {

template <typename Key, typename Val, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class SharedLruCache {
public:
	using key_type = Key;
	using mapped_type = Val;
	using hasher = KeyHash;
	using key_equal = KeyEqual;

	// A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	explicit SharedLruCache(size_t max_entries_p) : max_entries(max_entries_p) {
	}

	// Disable copy and move.
	SharedLruCache(const SharedLruCache &) = delete;
	SharedLruCache &operator=(const SharedLruCache &) = delete;

	~SharedLruCache() = default;

	// Insert `value` with key `key`. This will replace any previous entry with the same key.
	void Put(Key key, shared_ptr<Val> value) {
		lru_list.emplace_front(key);
		Entry new_entry {std::move(value), lru_list.begin()};
		auto key_cref = std::cref(lru_list.front());
		entry_map[key_cref] = std::move(new_entry);

		if (max_entries > 0 && lru_list.size() > max_entries) {
			const auto &stale_key = lru_list.back();
			entry_map.erase(stale_key);
			lru_list.pop_back();
		}
	}

	// Delete the entry with key `key`. Return true if the entry was found for `key`, false if the entry was not found.
	// In both cases, there is no entry with key `key` existed after the call.
	bool Delete(const Key &key) {
		auto it = entry_map.find(key);
		if (it == entry_map.end()) {
			return false;
		}
		lru_list.erase(it->second.lru_iterator);
		entry_map.erase(it);
		return true;
	}

	// Look up the entry with key `key`.
	// Return nullptr if `key` doesn't exist in cache.
	shared_ptr<Val> Get(const Key &key) {
		const auto entry_map_iter = entry_map.find(key);
		if (entry_map_iter == entry_map.end()) {
			return nullptr;
		}
		lru_list.splice(lru_list.begin(), lru_list, entry_map_iter->second.lru_iterator);
		return entry_map_iter->second.value;
	}

	// Clear the cache.
	void Clear() {
		entry_map.clear();
		lru_list.clear();
	}

	// Clear cache entry by its key functor.
	template <typename KeyFilter>
	void Clear(KeyFilter &&key_filter) {
		vector<Key> keys_to_delete;
		for (const auto &key : lru_list) {
			if (key_filter(key)) {
				keys_to_delete.emplace_back(key);
			}
		}
		for (const auto &key : keys_to_delete) {
			Delete(key);
		}
	}

	// Accessors for cache parameters.
	size_t MaxEntries() const {
		return max_entries;
	}

private:
	struct Entry {
		// The entry's value.
		shared_ptr<Val> value;

		// A list iterator pointing to the entry's position in the LRU list.
		typename std::list<Key>::iterator lru_iterator;
	};

	using KeyConstRef = std::reference_wrapper<const Key>;
	using EntryMap = std::unordered_map<KeyConstRef, Entry, RefHash<KeyHash>, RefEq<KeyEqual>>;

	// The maximum number of entries in the cache. A value of 0 means there is no limit on entry count.
	const size_t max_entries;

	// All keys are stored as refernce (`std::reference_wrapper`), and the ownership lies in `lru_list`.
	EntryMap entry_map;

	// The LRU list of entries. The front of the list identifies the most recently accessed entry.
	std::list<Key> lru_list;
};

// Same interfaces as `SharedLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using SharedLruConstCache = SharedLruCache<K, const V, KeyHash, KeyEqual>;

// Thread-safe implementation.
template <typename Key, typename Val, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class ThreadSafeSharedLruCache {
public:
	using key_type = Key;
	using mapped_type = Val;
	using hasher = KeyHash;
	using key_equal = KeyEqual;

	// A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	explicit ThreadSafeSharedLruCache(size_t max_entries) : internal_cache(max_entries) {
	}

	// Disable copy and move.
	ThreadSafeSharedLruCache(const ThreadSafeSharedLruCache &) = delete;
	ThreadSafeSharedLruCache &operator=(const ThreadSafeSharedLruCache &) = delete;

	~ThreadSafeSharedLruCache() = default;

	// Insert `value` with key `key`. This will replace any previous entry with the same key.
	void Put(Key key, shared_ptr<Val> value) {
		std::lock_guard<std::mutex> lock(mu);
		internal_cache.Put(std::move(key), std::move(value));
	}

	// Delete the entry with key `key`. Return true if the entry was found for `key`, false if the entry was not found.
	// In both cases, there is no entry with key `key` existed after the call.
	bool Delete(const Key &key) {
		std::lock_guard<std::mutex> lock(mu);
		return internal_cache.Delete(key);
	}

	// Look up the entry with key `key`.
	// Return nullptr if `key` doesn't exist in cache.
	shared_ptr<Val> Get(const Key &key) {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.Get(key);
	}

	// Clear the cache.
	void Clear() {
		std::lock_guard<std::mutex> lock(mu);
		internal_cache.Clear();
	}

	// Clear cache entry by its key functor.
	template <typename KeyFilter>
	void Clear(KeyFilter &&key_filter) {
		std::lock_guard<std::mutex> lock(mu);
		internal_cache.Clear(std::forward<KeyFilter>(key_filter));
	}

	// Accessors for cache parameters.
	size_t MaxEntries() const {
		return internal_cache.MaxEntries();
	}

	// Get or creation for cached key-value pairs.
	//
	// WARNING: Currently factory cannot have exception thrown.
	shared_ptr<Val> GetOrCreate(const Key &key, std::function<shared_ptr<Val>(const Key &)> factory) {
		shared_ptr<CreationToken> creation_token;

		{
			std::unique_lock<std::mutex> lck(mu);
			auto cached_val = internal_cache.Get(key);
			if (cached_val != nullptr) {
				return cached_val;
			}

			auto creation_iter = ongoing_creation.find(key);

			// Another thread has requested for the same key-value pair, simply wait for its completion.
			if (creation_iter != ongoing_creation.end()) {
				creation_token = creation_iter->second;
				++creation_token->count;
				creation_token->cv.wait(
				    lck, [creation_token = creation_token.get()]() { return creation_token->val != nullptr; });

				// Creation finished.
				--creation_token->count;
				if (creation_token->count == 0) {
					// [creation_iter] could be invalidated here due to new insertion/deletion.
					ongoing_creation.erase(key);
				}
				return creation_token->val;
			}

			// Current thread is the first one to request for the key-value pair, perform factory function.
			creation_iter = ongoing_creation.emplace(key, make_shared_ptr<CreationToken>()).first;
			creation_token = creation_iter->second;
			creation_token->count = 1;
		}

		// Place factory out of critical section.
		shared_ptr<Val> val = factory(key);

		{
			std::lock_guard<std::mutex> lck(mu);
			internal_cache.Put(key, val);
			creation_token->val = val;
			creation_token->cv.notify_all();
			int new_count = --creation_token->count;
			if (new_count == 0) {
				// [creation_iter] could be invalidated here due to new insertion/deletion.
				ongoing_creation.erase(key);
			}
		}

		return val;
	}

private:
	struct CreationToken {
		std::condition_variable cv;
		// Nullptr indicate creation unfinished.
		shared_ptr<Val> val;
		// Counter for ongoing creation.
		int count = 0;
	};

	std::mutex mu;
	SharedLruCache<Key, Val, KeyHash, KeyEqual> internal_cache;
	// Ongoing creation.
	std::unordered_map<Key, shared_ptr<CreationToken>, KeyHash, KeyEqual> ongoing_creation;
};

// Same interfaces as `SharedLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using ThreadSafeSharedLruConstCache = ThreadSafeSharedLruCache<K, const V, KeyHash, KeyEqual>;

} // namespace duckdb

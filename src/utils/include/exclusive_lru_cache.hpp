// ExclusiveLruCache is a LRU cache with all entries exclusive, which pops out at `GetAndPop` operation.
//
// It's made for values which indicate exclusive resource, for example, file handle.
//
// Example usage:
// ExclusiveLruCache<string, unique_ptr<FileHandle>> cache{/*max_entries_p=*/1, /*timeout_millisec_p=*/1000};
// cache.Put("hello", make_unique<FileHandle>(handle));
// auto cached_handle = cache.GetAndPop("hello");

#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <unordered_map>
#include <string>
#include <utility>
#include <type_traits>
#include <mutex>

#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
#include "map_utils.hpp"
#include "time_utils.hpp"

namespace duckdb {

// TODO(hjiang): The most ideal return type is `std::optional<Value>` for `GetAndPop`, but we're still at C++14, so have
// to use `unique_ptr`.
template <typename Key, typename Val, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class ExclusiveLruCache {
public:
	using key_type = Key;
	using mapped_type = Val;
	using hasher = KeyHash;
	using key_equal = KeyEqual;

	// @param max_entries_p: A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	// @param timeout_millisec_p: Timeout in milliseconds for entries, exceeding which invalidates the cache entries; 0
	// means no timeout.
	ExclusiveLruCache(size_t max_entries_p, uint64_t timeout_millisec_p)
	    : max_entries(max_entries_p), timeout_millisec(timeout_millisec_p) {
	}

	// Disable copy and move.
	ExclusiveLruCache(const ExclusiveLruCache &) = delete;
	ExclusiveLruCache &operator=(const ExclusiveLruCache &) = delete;

	~ExclusiveLruCache() = default;

	// Insert `value` with key `key`. This will replace any previous entry with the same key.
	// Return evicted value if any.
	//
	// Reasoning for returning the value back to caller:
	// 1. Caller is able to do processing for the value.
	// 2. For thread-safe lru cache, processing could be moved out of critical section.
	unique_ptr<Val> Put(Key key, unique_ptr<Val> value) {
		lru_list.emplace_front(key);
		Entry new_entry {
		    .value = std::move(value),
		    .timestamp = static_cast<uint64_t>(GetSteadyNowMilliSecSinceEpoch()),
		    .lru_iterator = lru_list.begin(),
		};
		auto key_cref = std::cref(lru_list.front());
		entry_map[key_cref] = std::move(new_entry);

		unique_ptr<Val> evicted_val = nullptr;
		if (max_entries > 0 && lru_list.size() > max_entries) {
			const auto &stale_key = lru_list.back();
			auto iter = entry_map.find(stale_key);
			D_ASSERT(iter != entry_map.end());
			evicted_val = std::move(iter->second.value);

			entry_map.erase(iter);
			lru_list.pop_back();
		}

		return evicted_val;
	}

	// Delete the entry with key `key`. Return true if the entry was found for `key`, false if the entry was not found.
	// In both cases, there is no entry with key `key` existed after the call.
	bool Delete(const Key &key) {
		auto it = entry_map.find(key);
		if (it == entry_map.end()) {
			return false;
		}
		DeleteImpl(it);
		return true;
	}

	// Look up the entry with key `key` and remove from cache.
	// Return nullptr if `key` doesn't exist in cache.
	unique_ptr<Val> GetAndPop(const Key &key) {
		const auto entry_map_iter = entry_map.find(key);
		if (entry_map_iter == entry_map.end()) {
			return nullptr;
		}

		// Check whether found cache entry is expired or not.
		if (timeout_millisec > 0) {
			const auto now = GetSteadyNowMilliSecSinceEpoch();
			if (now - entry_map_iter->second.timestamp > timeout_millisec) {
				DeleteImpl(entry_map_iter);
				return nullptr;
			}
		}

		// Move the value out and delete from LRU cache.
		unique_ptr<Val> value = std::move(entry_map_iter->second.value);
		DeleteImpl(entry_map_iter);
		return value;
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

	// Clear the cache and get all values, application could perform their processing logic upon these values.
	vector<unique_ptr<Val>> ClearAndGetValues() {
		vector<unique_ptr<Val>> values;
		values.reserve(entry_map.size());
		for (auto &[_, cur_entry] : entry_map) {
			values.emplace_back(std::move(cur_entry.value));
		}
		entry_map.clear();
		lru_list.clear();
		return values;
	}

	// Accessors for cache parameters.
	size_t MaxEntries() const {
		return max_entries;
	}

private:
	struct Entry {
		// The entry's value.
		unique_ptr<Val> value;

		// Steady clock timestamp when current entry was inserted into cache.
		// 1. It's not updated at later accesses.
		// 2. It's updated at replace update operations.
		uint64_t timestamp;

		// A list iterator pointing to the entry's position in the LRU list.
		typename std::list<Key>::iterator lru_iterator;
	};

	using KeyConstRef = std::reference_wrapper<const Key>;
	using EntryMap = std::unordered_map<KeyConstRef, Entry, RefHash<KeyHash>, RefEq<KeyEqual>>;

	// Delete key-value pairs indicated by the given entry map iterator [iter] from cache.
	unique_ptr<Val> DeleteImpl(typename EntryMap::iterator iter) {
		auto value = std::move(iter->second.value);
		lru_list.erase(iter->second.lru_iterator);
		entry_map.erase(iter);
		return value;
	}

	// The maximum number of entries in the cache. A value of 0 means there is no limit on entry count.
	const size_t max_entries;

	// The timeout in seconds for cache entries; entries with exceeding timeout would be invalidated.
	const uint64_t timeout_millisec;

	// All keys are stored as refernce (`std::reference_wrapper`), and the ownership lies in `lru_list`.
	EntryMap entry_map;

	// The LRU list of entries. The front of the list identifies the most recently accessed entry.
	std::list<Key> lru_list;
};

// Same interfaces as `ExclusiveLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using ExclusiveLruConstCache = ExclusiveLruCache<K, const V, KeyHash, KeyEqual>;

// Thread-safe implementation.
template <typename Key, typename Val, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class ThreadSafeExclusiveLruCache {
public:
	using key_type = Key;
	using mapped_type = Val;
	using hasher = KeyHash;
	using key_equal = KeyEqual;

	// @param max_entries_p: A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	// @param timeout_millisec_p: Timeout in milliseconds for entries, exceeding which invalidates the cache entries; 0
	// means no timeout.
	ThreadSafeExclusiveLruCache(size_t max_entries, uint64_t timeout_millisec)
	    : internal_cache(max_entries, timeout_millisec) {
	}

	// Disable copy and move.
	ThreadSafeExclusiveLruCache(const ThreadSafeExclusiveLruCache &) = delete;
	ThreadSafeExclusiveLruCache &operator=(const ThreadSafeExclusiveLruCache &) = delete;

	~ThreadSafeExclusiveLruCache() = default;

	// Insert `value` with key `key`. This will replace any previous entry with the same key.
	unique_ptr<Val> Put(Key key, unique_ptr<Val> value) {
		std::lock_guard<std::mutex> lock(mu);
		return internal_cache.Put(std::move(key), std::move(value));
	}

	// Delete the entry with key `key`. Return true if the entry was found for `key`, false if the entry was not found.
	// In both cases, there is no entry with key `key` existed after the call.
	bool Delete(const Key &key) {
		std::lock_guard<std::mutex> lock(mu);
		return internal_cache.Delete(key);
	}

	// Look up the entry with key `key` and remove from cache.
	// Return nullptr if `key` doesn't exist in cache.
	unique_ptr<Val> GetAndPop(const Key &key) {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.GetAndPop(key);
	}

	// Clear the cache and get all values, application could perform their processing logic upon these values.
	vector<unique_ptr<Val>> ClearAndGetValues() {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.ClearAndGetValues();
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

private:
	std::mutex mu;
	ExclusiveLruCache<Key, Val, KeyHash, KeyEqual> internal_cache;
};

// Same interfaces as `ExclusiveLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using ThreadSafeExclusiveLruConstCache = ThreadSafeExclusiveLruCache<K, const V, KeyHash, KeyEqual>;

} // namespace duckdb

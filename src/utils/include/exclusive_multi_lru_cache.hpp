// ExclusiveMultiLruCache is a LRU cache with all entries exclusive, which
// - pops out at `GetAndPop` operation;
// - allows multiple values for one single key.
//
// It's made for values which indicate exclusive resource, for example, file handle.
//
// Example usage:
// ExclusiveMultiLruCache<string, FileHandle> cache{/*max_entries_p=*/1, /*timeout_millisec_p=*/1000};
// cache.Put("hello", make_unique<FileHandle>(handle));
// auto cached_handle = cache.GetAndPop("hello");

#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <string>
#include <utility>
#include <type_traits>

#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
#include "time_utils.hpp"

namespace duckdb {

// TODO(hjiang): The most ideal return type is `std::optional<Value>` for `GetAndPop`, but we're still at C++14, so have
// to use `unique_ptr`.
template <typename Key, typename Val, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class ExclusiveMultiLruCache {
public:
	using key_type = Key;
	using mapped_type = vector<unique_ptr<Val>>;
	using hasher = KeyHash;
	using key_equal = KeyEqual;

	struct GetAndPopResult {
		// Entries evicted due to staleness.
		vector<unique_ptr<Val>> evicted_items;
		// The real entry caller could use.
		unique_ptr<Val> target_item;
	};

	// @param max_entries_p: A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	// @param timeout_millisec_p: Timeout in milliseconds for entries, exceeding which invalidates the cache entries; 0
	// means no timeout.
	ExclusiveMultiLruCache(size_t max_entries_p, uint64_t timeout_millisec_p)
	    : max_entries(max_entries_p), timeout_millisec(timeout_millisec_p) {
	}

	// Disable copy and move.
	ExclusiveMultiLruCache(const ExclusiveMultiLruCache &) = delete;
	ExclusiveMultiLruCache &operator=(const ExclusiveMultiLruCache &) = delete;

	~ExclusiveMultiLruCache() = default;

	// Insert `value` with key `key`, the values with the same key will be kept and evicted first.
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
		entry_map[key_cref].emplace_back(std::move(new_entry));
		++cur_entries_num;

		unique_ptr<Val> evicted_val = nullptr;
		if (max_entries > 0 && cur_entries_num > max_entries) {
			const auto &stale_key = lru_list.back();
			auto iter = entry_map.find(stale_key);
			D_ASSERT(iter != entry_map.end());
			evicted_val = DeleteFirstEntry(iter);
		}
		return evicted_val;
	}

	// Look up the entry with key `key` and remove from cache.
	// If there're multiple values corresponds to the given [key]. the oldest value will be returned.
	GetAndPopResult GetAndPop(const Key &key) {
		GetAndPopResult result;

		const auto entry_map_iter = entry_map.find(key);
		if (entry_map_iter == entry_map.end()) {
			return result;
		}

		// There're multiple entries correspond to the given [key], check whether they're stale one by one.
		auto &entries = entry_map_iter->second;
		if (timeout_millisec > 0) {
			const auto now = GetSteadyNowMilliSecSinceEpoch();
			size_t cur_entries_size = entries.size();
			while (cur_entries_num > 0) {
				auto &cur_entry = entries.front();
				if (now - cur_entry.timestamp > timeout_millisec) {
					result.evicted_items.emplace_back(DeleteFirstEntry(entry_map_iter));
					--cur_entries_size;
					continue;
				}
				break;
			}

			// If there're no left entries correspond to the given [key], we directly return.
			if (cur_entries_size == 0) {
				return result;
			}
		}

		// There're still fresh entry for the given [key].
		D_ASSERT(!entries.empty());
		result.target_item = std::move(entries.front().value);
		DeleteFirstEntry(entry_map_iter);
		return result;
	}

	// Clear the cache and get all values, application could perform their processing logic upon these values.
	vector<unique_ptr<Val>> ClearAndGetValues() {
		vector<unique_ptr<Val>> values;
		values.reserve(cur_entries_num);
		for (auto &[_, entries] : entry_map) {
			for (auto &cur_entry : entries) {
				values.emplace_back(std::move(cur_entry.value));
			}
		}
		entry_map.clear();
		lru_list.clear();
		return values;
	}

	// Check invariant:
	// - the number of entries in the LRU cache (1) = the number of entries in the entry map (2)
	// - the number of entries in the LRU cache (1) = the number of keys in lru list (3)
	//
	// This method iterates all elements in the LRU cache, which is time-consuming; it's supposed to be used in the unit
	// test and debug assertion.
	bool Verify() {
		// Count 2.
		int entry_map_count = 0;
		for (const auto &[_, entries] : entry_map) {
			if (entries.empty()) {
				return false;
			}
			entry_map_count += entries.size();
		}

		if (entry_map_count != cur_entries_num) {
			return false;
		}

		// Count 3.
		return lru_list.size() == cur_entries_num;
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

	using EntryMap = std::unordered_map<Key, std::deque<Entry>, KeyHash, KeyEqual>;

	// Delete the first entry from the given [iter], return the deleted entry.
	unique_ptr<Val> DeleteFirstEntry(typename EntryMap::iterator iter) {
		auto &entries = iter->second;
		D_ASSERT(!entries.empty());

		auto value = std::move(entries.front().value);
		lru_list.erase(entries.front().lru_iterator);
		if (entries.size() == 1) {
			entry_map.erase(iter);
		} else {
			entries.pop_front();
		}
		--cur_entries_num;
		return value;
	}

	// Current number of entries in the cache.
	size_t cur_entries_num = 0;

	// The maximum number of entries in the cache. A value of 0 means there is no limit on entry count.
	const size_t max_entries;

	// The timeout in seconds for cache entries; entries with exceeding timeout would be invalidated.
	const uint64_t timeout_millisec;

	// All keys are stored as refernce (`std::reference_wrapper`), and the ownership lies in `lru_list`.
	EntryMap entry_map;

	// The LRU list of entries. The front of the list identifies the most recently accessed entry.
	std::list<Key> lru_list;
};

// Same interfaces as `ExclusiveMultiLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using ExclusiveLruConstCache = ExclusiveMultiLruCache<K, const V, KeyHash, KeyEqual>;

// Thread-safe implementation.
template <typename Key, typename Val, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class ThreadSafeExclusiveMultiLruCache {
public:
	using key_type = Key;
	using mapped_type = Val;
	using hasher = KeyHash;
	using key_equal = KeyEqual;
	using GetAndPopResult = typename ExclusiveMultiLruCache<Key, Val, KeyHash, KeyEqual>::GetAndPopResult;

	// @param max_entries_p: A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	// @param timeout_millisec_p: Timeout in milliseconds for entries, exceeding which invalidates the cache entries; 0
	// means no timeout.
	ThreadSafeExclusiveMultiLruCache(size_t max_entries, uint64_t timeout_millisec)
	    : internal_cache(max_entries, timeout_millisec) {
	}

	// Disable copy and move.
	ThreadSafeExclusiveMultiLruCache(const ThreadSafeExclusiveMultiLruCache &) = delete;
	ThreadSafeExclusiveMultiLruCache &operator=(const ThreadSafeExclusiveMultiLruCache &) = delete;

	~ThreadSafeExclusiveMultiLruCache() = default;

	// Insert `value` with key `key`, the values with the same key will be kept and evicted first.
	// Return evicted value if any.
	unique_ptr<Val> Put(Key key, unique_ptr<Val> value) {
		std::lock_guard<std::mutex> lock(mu);
		return internal_cache.Put(std::move(key), std::move(value));
	}

	// Look up the entry with key `key` and remove from cache.
	// If there're multiple values corresponds to the given [key]. the oldest value will be returned.
	GetAndPopResult GetAndPop(const Key &key) {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.GetAndPop(key);
	}

	// Clear the cache and get all values, application could perform their processing logic upon these values.
	vector<unique_ptr<Val>> ClearAndGetValues() {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.ClearAndGetValues();
	}

	// Check invariant.
	bool Verify() {
		std::unique_lock<std::mutex> lock(mu);
		return internal_cache.Verify();
	}

private:
	std::mutex mu;
	ExclusiveMultiLruCache<Key, Val, KeyHash, KeyEqual> internal_cache;
};

// Same interfaces as `ExclusiveMultiLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using ThreadSafeExclusiveLruConstCache = ThreadSafeExclusiveMultiLruCache<K, const V, KeyHash, KeyEqual>;

} // namespace duckdb

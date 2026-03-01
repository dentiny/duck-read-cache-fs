// SharedValueLruCache is a LRU cache, with all entries shared, which allows each key value pair could be read from
// multiple requests.
//
// It's made for values which are expensive to copy, so we use shared pointer wrapper for all values.

#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <utility>
#include <type_traits>

#include "duckdb/common/helper.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "map_utils.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"
#include "time_utils.hpp"

namespace duckdb {

template <typename Key, typename Val, typename KeyEqual = std::equal_to<Key>>
class SharedValueLruCache {
public:
	using key_type = Key;
	using mapped_type = shared_ptr<Val>;
	using key_equal = KeyEqual;

	// @param max_entries_p: A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	// @param timeout_millisec_p: Timeout in milliseconds for entries, exceeding which invalidates the cache entries; 0
	// means no timeout.
	SharedValueLruCache(size_t max_entries_p, uint64_t timeout_millisec_p)
	    : max_entries(max_entries_p), timeout_millisec(timeout_millisec_p) {
	}

	// Disable copy and move.
	SharedValueLruCache(const SharedValueLruCache &) = delete;
	SharedValueLruCache &operator=(const SharedValueLruCache &) = delete;

	~SharedValueLruCache() = default;

	// Insert `value` with key `key`. This will replace any previous entry with the same key.
	void Put(Key key, shared_ptr<Val> value) {
		lru_list.emplace_front(key);
		Entry new_entry {
		    .value = std::move(value),
		    .timestamp = static_cast<uint64_t>(GetSteadyNowMilliSecSinceEpoch()),
		    .lru_iterator = lru_list.begin(),
		};
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
		DeleteImpl(it);
		return true;
	}

	// Look up the entry with key `key`.
	// Return nullptr if `key` doesn't exist in cache.
	shared_ptr<Val> Get(const Key &key) {
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

		lru_list.splice(lru_list.begin(), lru_list, entry_map_iter->second.lru_iterator);
		return entry_map_iter->second.value;
	}

	// Clear the cache.
	void Clear() {
		entry_map.clear();
		lru_list.clear();
	}

	// Clear cache entries, which matches [key_filter] starting from [start_key] inclusively.
	// It ends at the first non-matched entry.
	template <typename KeyFilter>
	void Clear(const Key& start_key, KeyFilter &&key_filter) {
		vector<Key> keys_to_delete;
		for (auto iter = entry_map.find(start_key); iter != entry_map.end(); ++iter) {
			if (!key_filter(key)) {
				break;
			}
			keys_to_delete.emplace_back(key);
		}
		for (const auto &key : keys_to_delete) {
			Delete(key);
		}
	}

	// Accessors for cache parameters.
	size_t MaxEntries() const {
		return max_entries;
	}

	// Get all keys inside of the cache; the order of keys returned is not deterministic.
	vector<Key> Keys() const {
		vector<Key> keys;
		keys.reserve(entry_map.size());

		// Iterate on map because list iteration hurts cache locality thus bad performance.
		for (const auto &[key_ref, _] : entry_map) {
			keys.emplace_back(key_ref.get());
		}
		return keys;
	}

private:
	struct Entry {
		// The entry's value.
		shared_ptr<Val> value;

		// Steady clock timestamp when current entry was inserted into cache.
		// 1. It's not updated at later accesses.
		// 2. It's updated at replace update operations.
		uint64_t timestamp;

		// A list iterator pointing to the entry's position in the LRU list.
		typename std::list<Key>::iterator lru_iterator;
	};

	using KeyConstRef = std::reference_wrapper<const Key>;
	using EntryMap = map<KeyConstRef, Entry, RefHash<KeyHash>, RefEq<KeyEqual>>;

	// Delete key-value pairs indicated by the given entry map iterator [iter] from cache.
	void DeleteImpl(typename EntryMap::iterator iter) {
		lru_list.erase(iter->second.lru_iterator);
		entry_map.erase(iter);
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

// Same interfaces as `SharedValueLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyEqual = std::equal_to<K>>
using SharedValueLruConstCache = SharedValueLruCache<K, const V, KeyEqual>;

// Thread-safe implementation.
template <typename Key, typename Val,typename KeyEqual = std::equal_to<Key>>
class ThreadSafeSharedValueLruCache {
public:
	using lru_impl = SharedValueLruCache<Key, Val, KeyEqual>;
	using key_type = typename lru_impl::key_type;
	using mapped_type = typename lru_impl::mapped_type;
	using key_equal = typename lru_impl::key_equal;

	// @param max_entries_p: A `max_entries` of 0 means that there is no limit on the number of entries in the cache.
	// @param timeout_millisec_p: Timeout in milliseconds for entries, exceeding which invalidates the cache entries; 0
	// means no timeout.
	ThreadSafeSharedValueLruCache(size_t max_entries, uint64_t timeout_millisec)
	    : internal_cache(max_entries, timeout_millisec) {
	}

	// Disable copy and move.
	ThreadSafeSharedValueLruCache(const ThreadSafeSharedValueLruCache &) = delete;
	ThreadSafeSharedValueLruCache &operator=(const ThreadSafeSharedValueLruCache &) = delete;

	~ThreadSafeSharedValueLruCache() = default;

	// Insert `value` with key `key`. This will replace any previous entry with the same key.
	void Put(Key key, shared_ptr<Val> value) {
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		internal_cache.Put(std::move(key), std::move(value));
	}

	// Delete the entry with key `key`. Return true if the entry was found for `key`, false if the entry was not found.
	// In both cases, there is no entry with key `key` existed after the call.
	bool Delete(const Key &key) {
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		return internal_cache.Delete(key);
	}

	// Look up the entry with key `key`.
	// Return nullptr if `key` doesn't exist in cache.
	shared_ptr<Val> Get(const Key &key) {
		concurrency::unique_lock<concurrency::mutex> lock(mu);
		return internal_cache.Get(key);
	}

	// Clear the cache.
	void Clear() {
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		internal_cache.Clear();
	}

	// Clear cache entries, which matches [key_filter] starting from [start_key] inclusively.
	// It ends at the first non-matched entry.
	template <typename KeyFilter>
	void Clear(const Key& start_key, KeyFilter &&key_filter) {
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		internal_cache.Clear(start_key, std::forward<KeyFilter>(key_filter));
	}

	// Accessors for cache parameters.
	size_t MaxEntries() const {
		return internal_cache.MaxEntries();
	}

	// Get all keys inside of the cache; the order of keys returned is not deterministic.
	vector<Key> Keys() const {
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		return internal_cache.Keys();
	}

	// Get or creation for cached key-value pairs.
	//
	// WARNING: Currently factory cannot have exception thrown.
	shared_ptr<Val> GetOrCreate(const Key &key, std::function<shared_ptr<Val>(const Key &)> factory) {
		shared_ptr<CreationToken> creation_token;

		{
			concurrency::unique_lock<concurrency::mutex> lck(mu);
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
			const concurrency::lock_guard<concurrency::mutex> lck(mu);
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

	mutable concurrency::mutex mu;
	SharedValueLruCache<Key, Val, KeyEqual> internal_cache DUCKDB_GUARDED_BY(mu);
	// Ongoing creation.
	unordered_map<Key, shared_ptr<CreationToken>, KeyEqual> ongoing_creation DUCKDB_GUARDED_BY(mu);
};

// Same interfaces as `SharedValueLruCache`, but all cached values are `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyEqual = std::equal_to<K>>
using ThreadSafeSharedValueLruConstCache = ThreadSafeSharedValueLruCache<K, const V, KeyEqual>;

} // namespace duckdb

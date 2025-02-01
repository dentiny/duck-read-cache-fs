// SharedLruCache is a LRU cache, with all entries shared, which allows each key
// value pair could be read from multiple requests.

#pragma once

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

#include "map_utils.hpp"

namespace duckdb {

template <typename Key, typename Val, typename KeyHash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
class SharedLruCache {
public:
  using key_type = Key;
  using mapped_type = Val;
  using hasher = KeyHash;
  using key_equal = KeyEqual;

  // A `max_entries` of 0 means that there is no limit on the number of entries
  // in the cache.
  explicit SharedLruCache(size_t max_entries) : max_entries_(max_entries) {}

  ~SharedLruCache() = default;

  // Insert `value` with key `key`. This will replace any previous entry with
  // the same key.
  void Put(Key key, std::shared_ptr<Val> value) {
    lru_list_.emplace_front(key);
    Entry new_entry{std::move(value), lru_list_.begin()};
    auto key_cref = std::cref(lru_list_.front());
    cache_[key_cref] = std::move(new_entry);

    if (max_entries_ > 0 && lru_list_.size() > max_entries_) {
      const auto &stale_key = lru_list_.back();
      cache_.erase(stale_key);
      lru_list_.pop_back();
    }
  }

  // Delete the entry with key `key`. Return true if the entry was found for
  // `key`, false if the entry was not found. In both cases, there is no entry
  // with key `key` existed after the call.
  bool Delete(const Key &key) {
    auto it = cache_.find(key);
    if (it == cache_.end()) {
      return false;
    }
    lru_list_.erase(it->second.lru_iterator);
    cache_.erase(it);
    return true;
  }

  // Look up the entry with key `key`.
  // Return nullptr if `key` doesn't exist in cache.
  std::shared_ptr<Val> Get(const Key &key) {
    const auto cache_iter = cache_.find(key);
    if (cache_iter == cache_.end()) {
      return nullptr;
    }
    lru_list_.splice(lru_list_.begin(), lru_list_,
                     cache_iter->second.lru_iterator);
    return cache_iter->second.value;
  }

  // Clear the cache.
  void Clear() {
    cache_.clear();
    lru_list_.clear();
  }

  // Accessors for cache parameters.
  size_t max_entries() const { return max_entries_; }

private:
  struct Entry {
    // The entry's value.
    std::shared_ptr<Val> value;

    // A list iterator pointing to the entry's position in the LRU list.
    typename std::list<Key>::iterator lru_iterator;
  };

  using KeyConstRef = std::reference_wrapper<const Key>;
  using EntryMap =
      std::unordered_map<KeyConstRef, Entry, RefHash<KeyHash>, RefEq<KeyEqual>>;

  // The maximum number of entries in the cache. A value of 0 means there is no
  // limit on entry count.
  const size_t max_entries_;

  // All keys are stored as refernce (`std::reference_wrapper`), and the
  // ownership lies in `lru_list_`.
  EntryMap cache_;

  // The LRU list of entries. The front of the list identifies the most
  // recently accessed entry.
  std::list<Key> lru_list_;
};

// Same interfaces as `SharedLruCache`, but all cached values are
// `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>,
          typename KeyEqual = std::equal_to<K>>
using SharedLruConstCache = SharedLruCache<K, const V, KeyHash, KeyEqual>;

// Thread-safe implementation.
template <typename Key, typename Val, typename KeyHash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
class ThreadSafeSharedLruCache {
public:
  using key_type = Key;
  using mapped_type = Val;
  using hasher = KeyHash;
  using key_equal = KeyEqual;

  // A `max_entries` of 0 means that there is no limit on the number of entries
  // in the cache.
  explicit ThreadSafeSharedLruCache(size_t max_entries) : cache_(max_entries) {}

  ~ThreadSafeSharedLruCache() = default;

  // Insert `value` with key `key`. This will replace any previous entry with
  // the same key.
  void Put(Key key, std::shared_ptr<Val> value) {
    std::lock_guard<std::mutex> lock(mu_);
    cache_.Put(std::move(key), std::move(value));
  }

  // Delete the entry with key `key`. Return true if the entry was found for
  // `key`, false if the entry was not found. In both cases, there is no entry
  // with key `key` existed after the call.
  bool Delete(const Key &key) {
    std::lock_guard<std::mutex> lock(mu_);
    return cache_.Delete(key);
  }

  // Look up the entry with key `key`.
  // Return nullptr if `key` doesn't exist in cache.
  std::shared_ptr<Val> Get(const Key &key) {
    std::unique_lock<std::mutex> lock(mu_);
    return cache_.Get(key);
  }

  // Clear the cache.
  void Clear() {
    std::lock_guard<std::mutex> lock(mu_);
    cache_.Clear();
  }

  // Accessors for cache parameters.
  size_t max_entries() const { return cache_.max_entries_; }

private:
  std::mutex mu_;
  SharedLruCache<Key, Val> cache_;
};

// Same interfaces as `SharedLruCache`, but all cached values are
// `const` specified to avoid concurrent updates.
template <typename K, typename V, typename KeyHash = std::hash<K>,
          typename KeyEqual = std::equal_to<K>>
using ThreadSafeSharedLruConstCache =
    ThreadSafeSharedLruCache<K, const V, KeyHash, KeyEqual>;

} // namespace duckdb

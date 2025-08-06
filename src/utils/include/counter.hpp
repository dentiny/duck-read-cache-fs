// A thread-safe counter.
//
// Example usage:
// Counter<string, KeyHash, KeyEquan> counter{};
// unsigned new_count = counter.Increment();
// unsigned new_count = counter.Decrement();

#pragma once

#include <mutex>
#include <unordered_map>
#include <utility>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

template <typename Key, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>> 
class Counter final {
public:
    using key_type = Key;
    using hasher = KeyHash;
    using key_equal = KeyEqual;

    Counter() = default;
    // Disable copy and move.
	Counter(const Counter &) = delete;
	Counter &operator=(const Counter &) = delete;
    ~Counter() = default;

    // Increment the count for the given [key], and return the new count.
    template <typename KeyLike>
    unsigned Increment(KeyLike&& key) {
        const auto new_count = ++counter[std::forward<KeyLike>(key)];
        return new_count;
    }

    // Decrement the count for the given [key], and return the new count.
    // Precondition: the key exist in the counter map, otherwise assertion failure.
    template <typename KeyLike>
    unsigned Decrement(KeyLike&& key) {
        auto iter = counter.find(key);
        D_ASSERT(iter != counter.end());
        unsigned new_count = --iter->second;
        if (new_count == 0) {
            counter.erase(iter);
        }
        return new_count;
    }

    // Get the count for the given [key].
    template <typename KeyLike>
    unsigned GetCount(KeyLike&& key) {
        auto iter = counter.find(key);
        if (iter == counter.end()) {
            return 0;
        }
        return iter->second;
    }

    // Clear the counter entries which matches the given [key_pred].
    template <typename KeyPred>
    void ClearCounter(KeyPred&& key_pred) {
        vector<Key> keys_to_delete;
        for (const auto& [key, _] : counter) {
            if (key_pred(key)) {
                keys_to_delete.emplace_back(key);
            }
        }

        for (const auto& cur_key : keys_to_delete) {
            counter.erase(cur_key);
        }
    }

private:
    std::unordered_map<Key, unsigned, KeyHash, KeyEqual> counter;
};

// Thread-safe implementation.
template <typename Key, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>> 
class ThreadSafeCounter final {
public:
    using key_type = Key;
    using hasher = KeyHash;
    using key_equal = KeyEqual;

    ThreadSafeCounter() = default;
    // Disable copy and move.
	ThreadSafeCounter(const ThreadSafeCounter &) = delete;
	ThreadSafeCounter &operator=(const ThreadSafeCounter &) = delete;
    ~ThreadSafeCounter() = default;

    // Increment the count for the given [key], and return the new count.
    template <typename KeyLike>
    unsigned Increment(KeyLike&& key) {
        std::lock_guard<std::mutex> lck(mu);
        return counter.Increment(std::forward<KeyLike>(key));
    }

    // Decrement the count for the given [key], and return the new count.
    // Precondition: the key exist in the counter map, otherwise assertion failure.
    template <typename KeyLike>
    unsigned Decrement(KeyLike&& key) {
        std::lock_guard<std::mutex> lck(mu);
        return counter.Decrement(std::forward<KeyLike>(key));
    }

    // Get the count for the given [key].
    template <typename KeyLike>
    unsigned GetCount(KeyLike&& key) {
        std::lock_guard<std::mutex> lck(mu);
        return counter.GetCount(std::forward<KeyLike>(key));
    }

    // Clear the counter entries which matches the given [key_pred].
    template <typename KeyPred>
    void ClearCounter(KeyPred&& key_pred) {
        std::lock_guard<std::mutex> lck(mu);
        counter.ClearCounter(std::forward<KeyPred>(key_pred));
    }

private:
    std::mutex mu;
    Counter<Key, KeyHash, KeyEqual> counter;
};

}  // namespace duckdb

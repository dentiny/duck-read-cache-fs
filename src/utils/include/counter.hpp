// A thread-safe counter.
//
// Example usage:
// Counter<string, KeyHash, KeyEquan> counter{};
// unsigned new_count = counter.Increment();
// unsigned new_count = counter.Decrement();

#pragma once

#include <utility>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

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
	unsigned Increment(KeyLike &&key) {
		const auto new_count = ++counter[std::forward<KeyLike>(key)];
		return new_count;
	}

	// Decrement the count for the given [key], and return the new count.
	// Precondition: the key exist in the counter map, otherwise assertion failure.
	template <typename KeyLike>
	unsigned Decrement(const KeyLike &key) {
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
	unsigned GetCount(const KeyLike &key) {
		auto iter = counter.find(key);
		if (iter == counter.end()) {
			return 0;
		}
		return iter->second;
	}

private:
	unordered_map<Key, unsigned, KeyHash, KeyEqual> counter;
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
	unsigned Increment(KeyLike &&key) {
		const concurrency::lock_guard<concurrency::mutex> lck(mu);
		return counter.Increment(std::forward<KeyLike>(key));
	}

	// Decrement the count for the given [key], and return the new count.
	// Precondition: the key exist in the counter map, otherwise assertion failure.
	template <typename KeyLike>
	unsigned Decrement(const KeyLike &key) {
		const concurrency::lock_guard<concurrency::mutex> lck(mu);
		return counter.Decrement(key);
	}

	// Get the count for the given [key].
	template <typename KeyLike>
	unsigned GetCount(const KeyLike &key) {
		const concurrency::lock_guard<concurrency::mutex> lck(mu);
		return counter.GetCount(key);
	}

private:
	concurrency::mutex mu;
	Counter<Key, KeyHash, KeyEqual> counter DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb

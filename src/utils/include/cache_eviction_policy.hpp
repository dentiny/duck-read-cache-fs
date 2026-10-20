// Eviction policy for bounded key-value caches.
//
// The cache owns the entries, while the policy only tracks keys and decides which of them to evict. Policies are not
// thread-safe, callers are expected to synchronize access together with the cache entries.

#pragma once

#include "duckdb/common/vector.hpp"

namespace duckdb {

template <typename Key>
class CacheEvictionPolicy {
public:
	virtual ~CacheEvictionPolicy() = default;

	// Start tracking [key], which must not be tracked already.
	// Return keys the cache should evict, which could include [key] itself if the policy rejects its admission.
	virtual vector<Key> OnInsert(const Key &key) = 0;

	// Record an access to the tracked [key], either a cache hit or an in-place value update.
	virtual void OnAccess(const Key &key) = 0;

	// Stop tracking [key], which is removed by the cache rather than evicted by the policy (i.e. deletion, expiration).
	virtual void OnRemove(const Key &key) = 0;

	// Stop tracking all keys, and reset all internal states.
	virtual void Clear() = 0;
};

} // namespace duckdb

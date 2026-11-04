// Eviction policy for bounded caches.
//
// The policy only tracks keys and decides which of them to evict; the cache owns the values, enforces its capacity, and
// performs the actual eviction. Policies are not thread-safe, callers are expected to synchronize access.

#pragma once

#include "optional.hpp"

namespace duckdb {

template <typename Key>
class EvictionPolicy {
public:
	virtual ~EvictionPolicy() = default;

	// Start tracking [key], which must not be tracked already.
	virtual void OnInsert(const Key &key) = 0;

	// Record an access to the tracked [key], either a cache hit or an in-place value update.
	virtual void OnAccess(const Key &key) = 0;

	// Stop tracking [key], which is removed by the cache rather than evicted by the policy (i.e. deletion, expiration).
	// [key] must be tracked.
	virtual void OnRemove(const Key &key) = 0;

	// Whether [key] is tracked.
	virtual bool Contains(const Key &key) const = 0;

	// Stop tracking the key to evict and return it; nullopt if no key is tracked.
	// The returned key could be the latest inserted one, if the policy rejects its admission.
	virtual optional<Key> PopVictim() = 0;

	// Stop tracking all keys, and reset all internal states.
	virtual void Clear() = 0;
};

} // namespace duckdb

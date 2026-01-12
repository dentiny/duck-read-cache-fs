// Wrapper around lock types in standard libraries.
// See: https://clang.llvm.org/docs/ThreadSafetyAnalysis.html#mutex-h

#pragma once

#include <chrono>
#include <mutex>

#include "internal/mutex_impl.hpp"
#include "thread_annotation.hpp"

namespace duckdb {
namespace concurrency {

template <typename M>
class DUCKDB_SCOPED_CAPABILITY lock_guard : public internal::lock_impl_t<lock_guard<M>> {
private:
	using Impl = internal::lock_impl_t<lock_guard<M>>;

public:
	explicit lock_guard(M &m) DUCKDB_ACQUIRE(m) : Impl(m) {
	}
	lock_guard(M &m, std::adopt_lock_t t) DUCKDB_REQUIRES(m) : Impl(m, t) {
	}
	~lock_guard() DUCKDB_RELEASE() = default;
};

template <typename M>
class DUCKDB_SCOPED_CAPABILITY unique_lock : public internal::lock_impl_t<unique_lock<M>> {
private:
	using Impl = internal::lock_impl_t<unique_lock<M>>;

public:
	unique_lock() = default;
	explicit unique_lock(M &m) DUCKDB_ACQUIRE(m) : Impl(m) {
	}
	unique_lock(M &m, std::defer_lock_t t) noexcept DUCKDB_EXCLUDES(m) : Impl(m, t) {
	}
	unique_lock(M &m, std::adopt_lock_t t) DUCKDB_REQUIRES(m) : Impl(m, t) {
	}
	unique_lock(M &m, std::try_to_lock_t t) DUCKDB_TRY_ACQUIRE(true, m) : Impl(m, t) {
	}
	~unique_lock() DUCKDB_RELEASE() = default;

	void lock() DUCKDB_ACQUIRE() {
		Impl::lock();
	}
	bool try_lock() DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock();
	}
	template <typename R, typename P>
	bool try_lock_for(const std::chrono::duration<R, P> &timeout) DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock_for(timeout);
	}
	template <typename C, typename D>
	bool try_lock_until(const std::chrono::time_point<C, D> &timeout) DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock_until(timeout);
	}
	void unlock() DUCKDB_RELEASE() {
		Impl::unlock();
	}
};

} // namespace concurrency
} // namespace duckdb

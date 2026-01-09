// Wrapper around mutex types within standard libraries.
// See: https://clang.llvm.org/docs/ThreadSafetyAnalysis.html#mutex-h

#pragma once

#include <mutex>
#include <shared_mutex>

#include "thread_annotation.hpp"
#include "internal/mutex_impl.hpp"

namespace duckdb {
namespace concurrency {

class DUCKDB_CAPABILITY("mutex") mutex : public internal::mutex_impl_t<mutex> {
private:
	using Impl = internal::mutex_impl_t<mutex>;

public:
	void lock() DUCKDB_ACQUIRE() {
		Impl::lock();
	}
	void unlock() DUCKDB_RELEASE() {
		Impl::unlock();
	}
	bool try_lock() DUCKDB_TRY_ACQUIRE(true) {
		return Impl::try_lock();
	}
};

} // namespace concurrency
} // namespace duckdb

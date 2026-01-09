// Bind annotated mutex and lock type with standard implementation, so that
// duckdb::concurrency::unique_lock<duckdb::concurrency::mutex> could inherit std::unique_lock<std::mutex>.

#pragma once

#include <mutex>

namespace duckdb {
namespace concurrency {

// Forward declaration for annotated mutex types.
class mutex;

// Forward declaration for annotated lock types.
template <typename M>
class unique_lock;
template <typename M>
class lock_guard;

namespace internal {

// Type alias for mutex types.
template <typename T>
struct standard_impl {
	using type = T;
};
template <typename T>
using standard_impl_t = typename standard_impl<T>::type;

// Mutex specialization.
template <>
struct standard_impl<::duckdb::concurrency::mutex> {
	using type = std::mutex;
};

// Type alias for lock types.
template <typename M>
using mutex_impl_t = standard_impl_t<M>;

// Lock specialization.
template <typename M>
struct standard_impl<::duckdb::concurrency::unique_lock<M>> {
	using type = std::unique_lock<mutex_impl_t<M>>;
};

template <typename M>
struct standard_impl<::duckdb::concurrency::lock_guard<M>> {
	using type = std::lock_guard<mutex_impl_t<M>>;
};

template <typename L>
using lock_impl_t = standard_impl_t<L>;

} // namespace internal

} // namespace concurrency
} // namespace duckdb

// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Adapted from abseil optional implementation.

#pragma once

#include <functional>
#include <initializer_list>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace duckdb {

// -----------------------------------------------------------------------------
// nullopt_t and nullopt
// -----------------------------------------------------------------------------
struct nullopt_t {
	explicit constexpr nullopt_t(int) {
	}
};

constexpr nullopt_t nullopt(0);

// -----------------------------------------------------------------------------
// in_place_t and in_place
// -----------------------------------------------------------------------------
struct in_place_t {
	explicit constexpr in_place_t(int) {
	}
};

constexpr in_place_t in_place(0);

// -----------------------------------------------------------------------------
// bad_optional_access
// -----------------------------------------------------------------------------
class bad_optional_access : public std::exception {
public:
	bad_optional_access() noexcept = default;
	const char *what() const noexcept override {
		return "bad optional access";
	}
};

// -----------------------------------------------------------------------------
// optional
// -----------------------------------------------------------------------------
template <typename T>
class optional {
public:
	typedef T value_type;

	// Constructors
	constexpr optional() noexcept : has_value_(false) {
	}

	constexpr optional(nullopt_t) noexcept : has_value_(false) {
	}

	optional(const optional &other) : has_value_(false) {
		if (other.has_value_) {
			construct(*other.data_ptr());
		}
	}

	optional(optional &&other) noexcept(std::is_nothrow_move_constructible<T>::value) : has_value_(false) {
		if (other.has_value_) {
			construct(std::move(*other.data_ptr()));
		}
	}

	template <typename U = T,
	          typename std::enable_if<!std::is_same<typename std::decay<U>::type, optional>::value &&
	                                      !std::is_same<typename std::decay<U>::type, in_place_t>::value &&
	                                      !std::is_same<typename std::decay<U>::type, nullopt_t>::value &&
	                                      std::is_constructible<T, U &&>::value,
	                                  int>::type = 0>
	optional(U &&value) : has_value_(false) {
		construct(std::forward<U>(value));
	}

	template <typename... Args, typename std::enable_if<std::is_constructible<T, Args &&...>::value, int>::type = 0>
	explicit optional(in_place_t, Args &&...args) : has_value_(false) {
		construct(std::forward<Args>(args)...);
	}

	template <
	    typename U, typename... Args,
	    typename std::enable_if<std::is_constructible<T, std::initializer_list<U> &, Args &&...>::value, int>::type = 0>
	explicit optional(in_place_t, std::initializer_list<U> il, Args &&...args) : has_value_(false) {
		construct(il, std::forward<Args>(args)...);
	}

	~optional() {
		reset();
	}

	// Assignment
	optional &operator=(nullopt_t) noexcept {
		reset();
		return *this;
	}

	optional &operator=(const optional &rhs) {
		if (this == &rhs)
			return *this;
		if (rhs.has_value_) {
			assign_or_construct(*rhs.data_ptr());
		} else {
			reset();
		}
		return *this;
	}

	optional &operator=(optional &&rhs) noexcept(std::is_nothrow_move_assignable<T>::value &&
	                                             std::is_nothrow_move_constructible<T>::value) {
		if (this == &rhs)
			return *this;
		if (rhs.has_value_) {
			assign_or_construct(std::move(*rhs.data_ptr()));
		} else {
			reset();
		}
		return *this;
	}

	template <typename U = T,
	          typename std::enable_if<!std::is_same<typename std::decay<U>::type, optional>::value &&
	                                      std::is_constructible<T, U &&>::value && std::is_assignable<T &, U &&>::value,
	                                  int>::type = 0>
	optional &operator=(U &&value) {
		assign_or_construct(std::forward<U>(value));
		return *this;
	}

	// Modifiers
	void reset() noexcept {
		if (has_value_) {
			data_ptr()->~T();
			has_value_ = false;
		}
	}

	template <typename... Args>
	T &emplace(Args &&...args) {
		reset();
		construct(std::forward<Args>(args)...);
		return value();
	}

	template <typename U, typename... Args>
	T &emplace(std::initializer_list<U> il, Args &&...args) {
		reset();
		construct(il, std::forward<Args>(args)...);
		return value();
	}

	void swap(optional &other) noexcept(std::is_nothrow_move_constructible<T>::value &&
	                                    std::is_nothrow_move_assignable<T>::value &&
	                                    noexcept(std::swap(std::declval<T &>(), std::declval<T &>()))) {
		using std::swap;
		if (has_value_ && other.has_value_) {
			swap(*data_ptr(), *other.data_ptr());
		} else if (has_value_ && !other.has_value_) {
			other.construct(std::move(*data_ptr()));
			reset();
		} else if (!has_value_ && other.has_value_) {
			construct(std::move(*other.data_ptr()));
			other.reset();
		}
	}

	// Observers
	constexpr bool has_value() const noexcept {
		return has_value_;
	}
	constexpr explicit operator bool() const noexcept {
		return has_value_;
	}

	T &value() & {
		if (!has_value_)
			throw bad_optional_access();
		return *data_ptr();
	}

	const T &value() const & {
		if (!has_value_)
			throw bad_optional_access();
		return *data_ptr();
	}

	T &&value() && {
		if (!has_value_)
			throw bad_optional_access();
		return std::move(*data_ptr());
	}

	const T &&value() const && {
		if (!has_value_)
			throw bad_optional_access();
		return std::move(*data_ptr());
	}

	T *operator->() {
		return data_ptr();
	}

	const T *operator->() const {
		return data_ptr();
	}

	T &operator*() & {
		return *data_ptr();
	}
	const T &operator*() const & {
		return *data_ptr();
	}
	T &&operator*() && {
		return std::move(*data_ptr());
	}
	const T &&operator*() const && {
		return std::move(*data_ptr());
	}

	template <typename U>
	T value_or(U &&default_value) const & {
		return has_value_ ? *data_ptr() : static_cast<T>(std::forward<U>(default_value));
	}

	template <typename U>
	T value_or(U &&default_value) && {
		return has_value_ ? std::move(*data_ptr()) : static_cast<T>(std::forward<U>(default_value));
	}

private:
	typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_;
	bool has_value_;

	T *data_ptr() {
		return reinterpret_cast<T *>(&storage_);
	}

	const T *data_ptr() const {
		return reinterpret_cast<const T *>(&storage_);
	}

	template <typename... Args>
	void construct(Args &&...args) {
		::new (static_cast<void *>(&storage_)) T(std::forward<Args>(args)...);
		has_value_ = true;
	}

	template <typename U>
	void assign_or_construct(U &&value) {
		if (has_value_) {
			*data_ptr() = std::forward<U>(value);
		} else {
			construct(std::forward<U>(value));
		}
	}
};

// -----------------------------------------------------------------------------
// Utility functions
// -----------------------------------------------------------------------------
template <typename T, typename std::enable_if<std::is_move_constructible<T>::value, int>::type = 0>
void swap(optional<T> &lhs, optional<T> &rhs) noexcept(noexcept(lhs.swap(rhs))) {
	lhs.swap(rhs);
}

template <typename T>
optional<typename std::decay<T>::type> make_optional(T &&value) {
	return optional<typename std::decay<T>::type>(std::forward<T>(value));
}

template <typename T, typename... Args>
optional<T> make_optional(Args &&...args) {
	return optional<T>(in_place, std::forward<Args>(args)...);
}

template <typename T, typename U, typename... Args>
optional<T> make_optional(std::initializer_list<U> il, Args &&...args) {
	return optional<T>(in_place, il, std::forward<Args>(args)...);
}

// -----------------------------------------------------------------------------
// Relational operators
// -----------------------------------------------------------------------------
template <typename T, typename U>
bool operator==(const optional<T> &lhs, const optional<U> &rhs) {
	if (static_cast<bool>(lhs) != static_cast<bool>(rhs))
		return false;
	return lhs ? (*lhs == *rhs) : true;
}

template <typename T, typename U>
bool operator!=(const optional<T> &lhs, const optional<U> &rhs) {
	return !(lhs == rhs);
}

template <typename T, typename U>
bool operator<(const optional<T> &lhs, const optional<U> &rhs) {
	if (!rhs)
		return false;
	if (!lhs)
		return true;
	return *lhs < *rhs;
}

template <typename T, typename U>
bool operator>(const optional<T> &lhs, const optional<U> &rhs) {
	return rhs < lhs;
}

template <typename T, typename U>
bool operator<=(const optional<T> &lhs, const optional<U> &rhs) {
	return !(rhs < lhs);
}

template <typename T, typename U>
bool operator>=(const optional<T> &lhs, const optional<U> &rhs) {
	return !(lhs < rhs);
}

// Comparison with nullopt
template <typename T>
bool operator==(const optional<T> &lhs, nullopt_t) noexcept {
	return !lhs;
}

template <typename T>
bool operator==(nullopt_t, const optional<T> &rhs) noexcept {
	return !rhs;
}

template <typename T>
bool operator!=(const optional<T> &lhs, nullopt_t) noexcept {
	return static_cast<bool>(lhs);
}

template <typename T>
bool operator!=(nullopt_t, const optional<T> &rhs) noexcept {
	return static_cast<bool>(rhs);
}

template <typename T>
bool operator<(const optional<T> &, nullopt_t) noexcept {
	return false;
}

template <typename T>
bool operator<(nullopt_t, const optional<T> &rhs) noexcept {
	return static_cast<bool>(rhs);
}

template <typename T>
bool operator<=(const optional<T> &lhs, nullopt_t) noexcept {
	return !lhs;
}

template <typename T>
bool operator<=(nullopt_t, const optional<T> &) noexcept {
	return true;
}

template <typename T>
bool operator>(const optional<T> &lhs, nullopt_t) noexcept {
	return static_cast<bool>(lhs);
}

template <typename T>
bool operator>(nullopt_t, const optional<T> &) noexcept {
	return false;
}

template <typename T>
bool operator>=(const optional<T> &, nullopt_t) noexcept {
	return true;
}

template <typename T>
bool operator>=(nullopt_t, const optional<T> &rhs) noexcept {
	return !rhs;
}

// Comparison with value
template <typename T, typename U>
bool operator==(const optional<T> &lhs, const U &rhs) {
	return lhs ? static_cast<bool>(*lhs == rhs) : false;
}

template <typename T, typename U>
bool operator==(const U &lhs, const optional<T> &rhs) {
	return rhs == lhs;
}

template <typename T, typename U>
bool operator!=(const optional<T> &lhs, const U &rhs) {
	return !(lhs == rhs);
}

template <typename T, typename U>
bool operator!=(const U &lhs, const optional<T> &rhs) {
	return !(rhs == lhs);
}

template <typename T, typename U>
bool operator<(const optional<T> &lhs, const U &rhs) {
	return lhs ? static_cast<bool>(*lhs < rhs) : true;
}

template <typename T, typename U>
bool operator<(const U &lhs, const optional<T> &rhs) {
	return rhs ? static_cast<bool>(lhs < *rhs) : false;
}

template <typename T, typename U>
bool operator<=(const optional<T> &lhs, const U &rhs) {
	return lhs ? static_cast<bool>(*lhs <= rhs) : true;
}

template <typename T, typename U>
bool operator<=(const U &lhs, const optional<T> &rhs) {
	return rhs ? static_cast<bool>(lhs <= *rhs) : false;
}

template <typename T, typename U>
bool operator>(const optional<T> &lhs, const U &rhs) {
	return lhs ? static_cast<bool>(*lhs > rhs) : false;
}

template <typename T, typename U>
bool operator>(const U &lhs, const optional<T> &rhs) {
	return rhs ? static_cast<bool>(lhs > *rhs) : true;
}

template <typename T, typename U>
bool operator>=(const optional<T> &lhs, const U &rhs) {
	return lhs ? static_cast<bool>(*lhs >= rhs) : false;
}

template <typename T, typename U>
bool operator>=(const U &lhs, const optional<T> &rhs) {
	return rhs ? static_cast<bool>(lhs >= *rhs) : true;
}

} // namespace duckdb

namespace std {

template <typename T>
struct hash<duckdb::optional<T>> {
	std::size_t operator()(const duckdb::optional<T> &opt) const {
		if (!opt) {
			return static_cast<std::size_t>(0x297814aaad196e6dULL);
		}
		return std::hash<T>()(*opt);
	}
};

} // namespace std

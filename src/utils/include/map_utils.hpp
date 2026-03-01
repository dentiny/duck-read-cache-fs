#pragma once

#include <functional>
#include <utility>

namespace duckdb {

// A hash wrapper made for `std::reference_wrapper`.
template <typename Hash>
struct RefHash : Hash {
	RefHash() = default;
	template <typename H>
	RefHash(H &&h) : Hash(std::forward<H>(h)) {
	} // NOLINT

	RefHash(const RefHash &) = default;
	RefHash(RefHash &&) noexcept = default;
	RefHash &operator=(const RefHash &) = default;
	RefHash &operator=(RefHash &&) noexcept = default;

	template <typename T>
	size_t operator()(std::reference_wrapper<const T> val) const {
		return Hash::operator()(val.get());
	}
	template <typename T>
	size_t operator()(const T &val) const {
		return Hash::operator()(val);
	}
};

// A hash equal wrapper made for `std::reference_wrapper`.
template <typename Equal>
struct RefEq : Equal {
	RefEq() = default;
	template <typename Eq>
	RefEq(Eq &&eq) : Equal(std::forward<Eq>(eq)) {
	} // NOLINT

	RefEq(const RefEq &) = default;
	RefEq(RefEq &&) noexcept = default;
	RefEq &operator=(const RefEq &) = default;
	RefEq &operator=(RefEq &&) noexcept = default;

	template <typename T1, typename T2>
	bool operator()(std::reference_wrapper<const T1> lhs, std::reference_wrapper<const T2> rhs) const {
		return Equal::operator()(lhs.get(), rhs.get());
	}
	template <typename T1, typename T2>
	bool operator()(const T1 &lhs, std::reference_wrapper<const T2> rhs) const {
		return Equal::operator()(lhs, rhs.get());
	}
	template <typename T1, typename T2>
	bool operator()(std::reference_wrapper<const T1> lhs, const T2 &rhs) const {
		return Equal::operator()(lhs.get(), rhs);
	}
	template <typename T1, typename T2>
	bool operator()(const T1 &lhs, const T2 &rhs) const {
		return Equal::operator()(lhs, rhs);
	}
};

// A comparison wrapper for std::reference_wrapper<const T> for use as std::map comparator.
// Supports transparent lookup so find(key) works when the map key type is reference_wrapper<const Key>.
template <typename Compare>
struct RefLess : Compare {
	using is_transparent = void;

	RefLess() = default;
	template <typename C>
	RefLess(C &&c) : Compare(std::forward<C>(c)) {
	} // NOLINT

	RefLess(const RefLess &) = default;
	RefLess(RefLess &&) noexcept = default;
	RefLess &operator=(const RefLess &) = default;
	RefLess &operator=(RefLess &&) noexcept = default;

	template <typename T>
	bool operator()(std::reference_wrapper<const T> lhs, std::reference_wrapper<const T> rhs) const {
		return Compare::operator()(lhs.get(), rhs.get());
	}
	template <typename T>
	bool operator()(const T &lhs, std::reference_wrapper<const T> rhs) const {
		return Compare::operator()(lhs, rhs.get());
	}
	template <typename T>
	bool operator()(std::reference_wrapper<const T> lhs, const T &rhs) const {
		return Compare::operator()(lhs.get(), rhs);
	}
};

} // namespace duckdb

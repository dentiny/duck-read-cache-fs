#include "base_profile_collector.hpp"

namespace duckdb {

namespace {
constexpr bool IsNonEmptyString(const char *str) {
	return str != nullptr && str[0] != '\0';
}

template <typename T, size_t N>
constexpr int ValidateStringArray(const std::array<T, N> &arr) {
	for (std::size_t idx = 0; idx < N; ++idx) {
		if (!IsNonEmptyString(arr[idx])) {
			return static_cast<int>(idx);
		}
	}
	return -1; // Indicates valid.
}

static_assert(
    ValidateStringArray<const char *, BaseProfileCollector::kIoOperationCount>(BaseProfileCollector::OPER_NAMES) == -1,
    "Operation name string array is invalid.");
static_assert(ValidateStringArray<const char *, BaseProfileCollector::kCacheEntityCount>(
                  BaseProfileCollector::CACHE_ENTITY_NAMES) == -1,
              "Cache entity string array is invalid.");
} // namespace
} // namespace duckdb

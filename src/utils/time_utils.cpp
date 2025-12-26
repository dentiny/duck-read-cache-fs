#include "time_utils.hpp"

#include <chrono>

namespace {
constexpr uint64_t kMicrosToNanos = 1000ULL;
constexpr uint64_t kSecondsToMicros = 1000ULL * 1000ULL;
constexpr uint64_t kSecondsToNanos = 1000ULL * 1000ULL * 1000ULL;
constexpr uint64_t kMilliToNanos = 1000ULL * 1000ULL;
} // namespace

namespace duckdb {

int64_t GetSteadyNowNanoSecSinceEpoch() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	    .count();
}

int64_t GetSteadyNowMilliSecSinceEpoch() {
	return GetSteadyNowNanoSecSinceEpoch() / kMilliToNanos;
}

int64_t GetSystemNowNanoSecSinceEpoch() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

int64_t GetSystemNowMilliSecSinceEpoch() {
	return GetSystemNowNanoSecSinceEpoch() / kMilliToNanos;
}

} // namespace duckdb

// Time related utils.

#pragma once

#include "duckdb/common/types/timestamp.hpp"

#include <cstdint>
#include <ctime>

namespace duckdb {

// Get current timestamp in steady clock since epoch in nanoseconds.
int64_t GetSteadyNowNanoSecSinceEpoch();

// Get current timestamp in steady clock since epoch in milliseconds.
int64_t GetSteadyNowMilliSecSinceEpoch();

// Get current timestamp in steady clock since epoch in nanoseconds.
int64_t GetSystemNowNanoSecSinceEpoch();

// Get current timestamp in steady clock since epoch in milliseconds.
int64_t GetSystemNowMilliSecSinceEpoch();

} // namespace duckdb

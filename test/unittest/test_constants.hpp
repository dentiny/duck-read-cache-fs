// This file contains test constants used across multiple unit tests.

#pragma once

#include <cstdint>

#include "duckdb/common/string.hpp"

namespace duckdb {

// Standard test file constants used across multiple unit tests
constexpr uint64_t TEST_FILE_SIZE = 26;
extern const string TEST_FILE_CONTENT;

} // namespace duckdb

// This file contains test constants used across multiple unit tests.

#pragma once

#include <cstdint>

#include "duckdb/common/string.hpp"

namespace duckdb {

// Standard test file constants used across multiple unit tests
constexpr uint64_t TEST_FILE_SIZE = 26;
const auto TEST_FILE_CONTENT = []() {
	string content(TEST_FILE_SIZE, '\0');
	for (uint64_t idx = 0; idx < TEST_FILE_SIZE; ++idx) {
		content[idx] = 'a' + idx;
	}
	return content;
}();

} // namespace duckdb

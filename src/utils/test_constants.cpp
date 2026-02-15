#include "test_constants.hpp"

namespace duckdb {

// Definition of TEST_FILE_CONTENT
const string TEST_FILE_CONTENT = []() {
	string content(TEST_FILE_SIZE, '\0');
	for (uint64_t idx = 0; idx < TEST_FILE_SIZE; ++idx) {
		content[idx] = 'a' + idx;
	}
	return content;
}();

} // namespace duckdb

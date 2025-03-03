#include "string_utils.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

bool operator==(const ImmutableBuffer &buffer1, const std::string &buffer2) {
	if (buffer1.Size() != buffer2.length()) {
		return false;
	}
	for (size_t idx = 0; idx < buffer1.Size(); ++idx) {
		if (buffer1.Data()[idx] != buffer2[idx]) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

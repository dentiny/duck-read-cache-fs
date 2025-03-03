// Data structure used for immutable buffer.
// Compared with other buffer representations like `std::vector<char>` and `std::string` it has a few advantages:
// - It supports creation with no initialization (aka, no `memset`).
// - It's cheap to copy and move.
// - One pointer indirection to actual content.

#pragma once

#include <cstddef>
#include <string>
#include <memory>

namespace duckdb {

struct ImmutableBuffer {
	std::shared_ptr<char[]> buffer;
	std::size_t buf_size;

	ImmutableBuffer(std::size_t size) : buffer(new char[size]), buf_size(size) {
	}

	// Get pointer to content.
	const char *data() const {
		return buffer.get();
	}
	// Get size of the buffer.
	std::size_t size() const {
		return buf_size;
	}
	// Whether the buffer is empty.
	bool empty() const {
		return buf_size == 0;
	}
};

// Compare with std::string.
bool operator==(const ImmutableBuffer &buffer1, const std::string &buffer2);
inline bool operator==(const std::string &buffer1, const ImmutableBuffer &buffer2) {
	return buffer2 == buffer1;
}
inline bool operator!=(const ImmutableBuffer &buffer1, const std::string &buffer2) {
	return !(buffer1 == buffer2);
}
inline bool operator!=(const std::string &buffer1, const ImmutableBuffer &buffer2) {
	return !(buffer1 == buffer2);
}

} // namespace duckdb

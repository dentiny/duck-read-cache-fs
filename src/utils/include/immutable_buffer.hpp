// Data structure used for immutable buffer.
// Compared with other buffer representations like `std::vector<char>`, `std::string` and `std::shared_ptr<std::string>`
// it has a few advantages:
// - It supports creation with no initialization (aka, no `memset`).
// - It's cheap to copy and move.
// - One pointer indirection to actual content.
// - One heap allocation.

#pragma once

#include <cstddef>
#include <memory>
#include <string>

namespace duckdb {

struct ImmutableBuffer {
	std::shared_ptr<char> buffer;
	std::size_t buf_size;

	ImmutableBuffer() : buffer(nullptr), buf_size(0) {
	}
	ImmutableBuffer(std::size_t size) : buffer(new char[size], std::default_delete<char[]>()), buf_size(size) {
	}

	ImmutableBuffer(const ImmutableBuffer &) = default;
	ImmutableBuffer &operator=(const ImmutableBuffer &) = default;
	ImmutableBuffer(ImmutableBuffer &&) = default;
	ImmutableBuffer &operator=(ImmutableBuffer &&) = default;

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
// Data structure used for both in-memory and on-disk cache reader.

#pragma once

#include <cstring>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

// All read requests are split into chunks, and executed in parallel.
// A [CacheReadChunk] represents a chunked IO request and its corresponding partial IO request.
struct CacheReadChunk {
	// Requested memory address and file offset to read from for current chunk.
	char *requested_start_addr = nullptr;
	idx_t requested_start_offset = 0;
	// Block size aligned [requested_start_offset].
	idx_t aligned_start_offset = 0;

	// Number of bytes for the chunk for IO operations, apart from the last chunk it's always cache block size.
	idx_t chunk_size = 0;

	// Allocated to store bytes.
	string content;
	// Number of bytes to copy from [content] to requested memory address.
	idx_t bytes_to_copy = 0;

	char *GetAddressToReadTo() const {
		return const_cast<char *>(content.data());
	}

	// Copy from [content] to application-provided buffer.
	void CopyBufferToRequestedMemory() {
		D_ASSERT(!content.empty());
		const idx_t delta_offset = requested_start_offset - aligned_start_offset;
		std::memmove(requested_start_addr, const_cast<char *>(content.data()) + delta_offset, bytes_to_copy);
	}

	// Copy from [buffer] to application-provided buffer.
	void CopyBufferToRequestedMemory(const string &buffer) {
		const idx_t delta_offset = requested_start_offset - aligned_start_offset;
		std::memmove(requested_start_addr, const_cast<char *>(buffer.data()) + delta_offset, bytes_to_copy);
	}

	// Take as memory buffer. [content] is not usable afterwards.
	string TakeAsBuffer() {
		D_ASSERT(!content.empty());
		auto buffer = std::move(content);
		return buffer;
	}
};

} // namespace duckdb

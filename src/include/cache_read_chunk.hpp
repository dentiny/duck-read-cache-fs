// Data structure used for both in-memory and on-disk cache reader.

#pragma once

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

	// Number of bytes to copy from [content] to requested memory address.
	idx_t bytes_to_copy = 0;

	// Copy from [buffer] to application-provided buffer.
	void CopyBufferToRequestedMemory(const string &buffer);
};

} // namespace duckdb

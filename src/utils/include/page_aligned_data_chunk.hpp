// Page-aligned uninitialized buffer for direct IO and cache chunks.

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

struct PageAlignedDeleter {
	void operator()(char *p) const noexcept;
};

// Page-aligned uninitialized data chunk.
struct PageAlignedDataChunk {
	unique_ptr<char[], PageAlignedDeleter> chunk;
	// Number of valid bytes (e.g. bytes to write or copy); may be less than capacity.
	idx_t length = 0;
	// Allocated size in bytes (always page-size aligned for direct IO).
	idx_t capacity = 0;

	// Pointer to the first byte (page-aligned). Undefined if chunk is null.
	char *data() {
		return chunk.get();
	}
	const char *data() const {
		return chunk.get();
	}
	bool empty() const {
		return chunk == nullptr || length == 0;
	}

	// Copy [copy_length] bytes from this chunk at [src_offset] to [dest].
	// The caller must ensure src_offset + copy_length <= length.
	void CopyTo(char *dest, idx_t src_offset, idx_t copy_length) const;
};

// Allocates a page-aligned buffer with capacity for at least [requested_length] bytes.
// Capacity is rounded up to page size for direct IO. Memory is uninitialized.
// length is initialized to 0; set it manually after filling the buffer.
PageAlignedDataChunk AllocatePageAlignedChunk(idx_t requested_length);

} // namespace duckdb

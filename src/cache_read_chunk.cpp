#include "cache_read_chunk.hpp"

#include <cstring>

namespace duckdb {

void CacheReadChunk::CopyBufferToRequestedMemory(const PageAlignedDataChunk &buffer) {
	const idx_t delta_offset = requested_start_offset - aligned_start_offset;
	buffer.CopyTo(requested_start_addr, delta_offset, bytes_to_copy);
}

} // namespace duckdb

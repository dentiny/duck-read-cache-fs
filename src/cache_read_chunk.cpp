#include "cache_read_chunk.hpp"

#include <cstring>

namespace duckdb {

void CacheReadChunk::CopyBufferToRequestedMemory(const string &buffer) {
	const idx_t delta_offset = requested_start_offset - aligned_start_offset;
	std::memmove(requested_start_addr, const_cast<char *>(buffer.data()) + delta_offset, bytes_to_copy);
}

} // namespace duckdb

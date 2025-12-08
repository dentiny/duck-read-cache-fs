#include "chunk_utils.hpp"

namespace duckdb {

ChunkAlignmentInfo CalculateChunkAlignment(const ReadRequestParams &params) {
	const idx_t aligned_start_offset = params.requested_start_offset / params.block_size * params.block_size;
	const idx_t aligned_last_chunk_offset =
	    (params.requested_start_offset + params.requested_bytes_to_read - 1) / params.block_size * params.block_size;
	const idx_t subrequest_count = (aligned_last_chunk_offset - aligned_start_offset) / params.block_size + 1;

	return ChunkAlignmentInfo {
	    .aligned_start_offset = aligned_start_offset,
	    .aligned_last_chunk_offset = aligned_last_chunk_offset,
	    .subrequest_count = subrequest_count,
	};
}

} // namespace duckdb

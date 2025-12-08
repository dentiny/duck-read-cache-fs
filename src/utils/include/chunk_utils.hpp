// Utility functions for calculating and processing cache read chunks.

#pragma once

#include "cache_read_chunk.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

// Parameters for a read request.
struct ReadRequestParams {
	// The start offset of the read request.
	idx_t requested_start_offset;
	// The number of bytes to read.
	idx_t requested_bytes_to_read;
	// The cache block size.
	idx_t block_size;
};

// Chunk alignment information for a read request.
struct ChunkAlignmentInfo {
	// Block size aligned start offset of the first chunk.
	idx_t aligned_start_offset;
	// Block size aligned start offset of the last chunk that contains any of the requested data.
	idx_t aligned_last_chunk_offset;
	// Number of chunks needed to fulfill the request.
	idx_t subrequest_count;
};

// Calculate chunk alignment information for a read request.
ChunkAlignmentInfo CalculateChunkAlignment(const ReadRequestParams &params);

} // namespace duckdb

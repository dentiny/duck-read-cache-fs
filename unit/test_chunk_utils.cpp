#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "chunk_utils.hpp"

using namespace duckdb; // NOLINT

// Test single chunk case: request fits entirely within one block.
TEST_CASE("Test single chunk alignment", "[chunk utils test]") {
	constexpr idx_t block_size = 10;
	constexpr idx_t requested_start_offset = 0;
	constexpr idx_t requested_bytes_to_read = 10;

	const ReadRequestParams params {
	    .requested_start_offset = requested_start_offset,
	    .requested_bytes_to_read = requested_bytes_to_read,
	    .block_size = block_size,
	};

	const ChunkAlignmentInfo alignment = CalculateChunkAlignment(params);

	REQUIRE(alignment.aligned_start_offset == 0);
	REQUIRE(alignment.aligned_last_chunk_offset == 0);
	REQUIRE(alignment.subrequest_count == 1);
}

// Test single chunk case with partial read: request is smaller than block size.
TEST_CASE("Test single chunk alignment with partial read", "[chunk utils test]") {
	constexpr idx_t block_size = 10;
	constexpr idx_t requested_start_offset = 0;
	constexpr idx_t requested_bytes_to_read = 5;

	const ReadRequestParams params {
	    .requested_start_offset = requested_start_offset,
	    .requested_bytes_to_read = requested_bytes_to_read,
	    .block_size = block_size,
	};

	const ChunkAlignmentInfo alignment = CalculateChunkAlignment(params);

	REQUIRE(alignment.aligned_start_offset == 0);
	REQUIRE(alignment.aligned_last_chunk_offset == 0);
	REQUIRE(alignment.subrequest_count == 1);
}

// Test multiple chunks: request spans exactly two blocks.
TEST_CASE("Test multiple chunks spanning two blocks", "[chunk utils test]") {
	constexpr idx_t block_size = 10;
	constexpr idx_t requested_start_offset = 0;
	constexpr idx_t requested_bytes_to_read = 15;

	const ReadRequestParams params {
	    .requested_start_offset = requested_start_offset,
	    .requested_bytes_to_read = requested_bytes_to_read,
	    .block_size = block_size,
	};

	const ChunkAlignmentInfo alignment = CalculateChunkAlignment(params);

	REQUIRE(alignment.aligned_start_offset == 0);
	REQUIRE(alignment.aligned_last_chunk_offset == 10);
	REQUIRE(alignment.subrequest_count == 2);
}

// Test unaligned start offset: request starts in the middle of a block.
TEST_CASE("Test unaligned start offset", "[chunk utils test]") {
	constexpr idx_t block_size = 10;
	constexpr idx_t requested_start_offset = 5;
	constexpr idx_t requested_bytes_to_read = 10;

	const ReadRequestParams params {
	    .requested_start_offset = requested_start_offset,
	    .requested_bytes_to_read = requested_bytes_to_read,
	    .block_size = block_size,
	};

	const ChunkAlignmentInfo alignment = CalculateChunkAlignment(params);

	REQUIRE(alignment.aligned_start_offset == 0);
	REQUIRE(alignment.aligned_last_chunk_offset == 10);
	REQUIRE(alignment.subrequest_count == 2);
}

// Test exact block boundary: request starts and ends at block boundaries.
TEST_CASE("Test exact block boundaries", "[chunk utils test]") {
	constexpr idx_t block_size = 10;
	constexpr idx_t requested_start_offset = 10;
	constexpr idx_t requested_bytes_to_read = 20;

	const ReadRequestParams params {
	    .requested_start_offset = requested_start_offset,
	    .requested_bytes_to_read = requested_bytes_to_read,
	    .block_size = block_size,
	};

	const ChunkAlignmentInfo alignment = CalculateChunkAlignment(params);

	REQUIRE(alignment.aligned_start_offset == 10);
	REQUIRE(alignment.aligned_last_chunk_offset == 20);
	REQUIRE(alignment.subrequest_count == 2);
}

// Test edge case: request ends exactly at block boundary.
TEST_CASE("Test request ending at block boundary", "[chunk utils test]") {
	constexpr idx_t block_size = 10;
	constexpr idx_t requested_start_offset = 5;
	constexpr idx_t requested_bytes_to_read = 5;

	const ReadRequestParams params {
	    .requested_start_offset = requested_start_offset,
	    .requested_bytes_to_read = requested_bytes_to_read,
	    .block_size = block_size,
	};

	const ChunkAlignmentInfo alignment = CalculateChunkAlignment(params);

	REQUIRE(alignment.aligned_start_offset == 0);
	REQUIRE(alignment.aligned_last_chunk_offset == 0);
	REQUIRE(alignment.subrequest_count == 1);
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}

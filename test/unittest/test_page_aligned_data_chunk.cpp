// Unit test for PageAlignedDataChunk: exercises CopyTo(dest, src_offset, copy_length).

#include "catch/catch.hpp"

#include "duckdb/common/vector.hpp"
#include "filesystem_utils.hpp"
#include "page_aligned_data_chunk.hpp"

#include <cstring>

using namespace duckdb;

TEST_CASE("PageAlignedDataChunk CopyTo copies data to destination", "[page_aligned_data_chunk]") {
	const idx_t page_size = GetFileSystemPageSize();
	const string expected = "hello world";

	auto chunk = AllocatePageAlignedChunk(page_size);
	std::memcpy(chunk.data(), expected.data(), expected.length());
	chunk.length = expected.length();

	vector<char> dest(expected.length(), 0);
	chunk.CopyTo(dest.data(), /*src_offset=*/0, expected.length());

	REQUIRE(std::memcmp(dest.data(), expected.data(), expected.length()) == 0);
}

// Unit test for PageAlignedDataChunk: exercises CopyFrom(pointer, length).

#include "catch/catch.hpp"

#include "duckdb/common/string_util.hpp"
#include "filesystem_utils.hpp"
#include "page_aligned_data_chunk.hpp"

#include <cstring>

using namespace duckdb;

TEST_CASE("PageAlignedDataChunk CopyFrom copies data and sets length", "[page_aligned_data_chunk]") {
	const idx_t page_size = GetFileSystemPageSize();
	const string source = "hello world";

	auto chunk = AllocatePageAlignedChunk(page_size);
	REQUIRE(chunk.length == 0);
	REQUIRE(chunk.capacity >= page_size);

	chunk.CopyFrom(source.data(), source.length());

	REQUIRE(chunk.length == source.length());
	REQUIRE(std::memcmp(chunk.data(), source.data(), source.length()) == 0);
}

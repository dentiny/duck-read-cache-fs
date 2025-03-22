#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <string>

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "mock_filesystem.hpp"

using namespace duckdb; // NOLINT

namespace {
const std::string TEST_FILENAME = "filename";
constexpr int64_t TEST_FILESIZE = 26;
constexpr int64_t TEST_CHUNK_SIZE = 5;

void TestReadWithMockFileSystem() {
	auto mock_filesystem = make_uniq<MockFileSystem>();
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	auto *mock_filesystem_ptr = mock_filesystem.get();
	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem));

	// Uncached read.
	{
		// Make sure it's mock file handle.
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto &cache_file_handle = handle->Cast<CacheFileSystemHandle>();
		[[maybe_unused]] auto &mock_file_handle = cache_file_handle.internal_file_handle->Cast<MockFileHandle>();

		std::string buffer(TEST_FILESIZE, '\0');
		cache_filesystem->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, /*location=*/0);
		REQUIRE(buffer == std::string(TEST_FILESIZE, 'a'));

		auto read_operations = mock_filesystem_ptr->GetSortedReadOperations();
		REQUIRE(read_operations.size() == 6);
		for (idx_t idx = 0; idx < 6; ++idx) {
			REQUIRE(read_operations[idx].start_offset == idx * TEST_CHUNK_SIZE);
			REQUIRE(read_operations[idx].bytes_to_read ==
			        MinValue<int64_t>(TEST_CHUNK_SIZE, TEST_FILESIZE - idx * TEST_CHUNK_SIZE));
		}
	}

	// Cache read.
	{
		// Make sure it's mock file handle.
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto &cache_file_handle = handle->Cast<CacheFileSystemHandle>();
		[[maybe_unused]] auto &mock_file_handle = cache_file_handle.internal_file_handle->Cast<MockFileHandle>();

		std::string buffer(TEST_FILESIZE, '\0');
		cache_filesystem->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, /*location=*/0);
		REQUIRE(buffer == std::string(TEST_FILESIZE, 'a'));

		mock_filesystem_ptr->ClearReadOperations();
		auto read_operations = mock_filesystem_ptr->GetSortedReadOperations();
		REQUIRE(read_operations.empty());
	}
}

} // namespace

TEST_CASE("Test disk cache reader with mock filesystem", "[mock filesystem test]") {
	*g_test_cache_type = *ON_DISK_CACHE_TYPE;
	g_cache_block_size = TEST_CHUNK_SIZE;
	g_max_file_handle_cache_entry = 1;
	LocalFileSystem::CreateLocal()->RemoveDirectory(*g_on_disk_cache_directory);
	TestReadWithMockFileSystem();
}

TEST_CASE("Test in-memory cache reader with mock filesystem", "[mock filesystem test]") {
	*g_test_cache_type = *IN_MEM_CACHE_TYPE;
	g_cache_block_size = TEST_CHUNK_SIZE;
	g_max_file_handle_cache_entry = 1;
	LocalFileSystem::CreateLocal()->RemoveDirectory(*g_on_disk_cache_directory);
	TestReadWithMockFileSystem();
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

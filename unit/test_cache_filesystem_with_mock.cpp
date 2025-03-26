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
const std::string TEST_GLOB_NAME = "*"; // Need to contain glob characters.
constexpr int64_t TEST_FILESIZE = 26;
constexpr int64_t TEST_CHUNK_SIZE = 5;

void TestReadWithMockFileSystem() {
	uint64_t close_invocation = 0;
	uint64_t dtor_invocation = 0;
	auto close_callback = [&close_invocation]() {
		++close_invocation;
	};
	auto dtor_callback = [&dtor_invocation]() {
		++dtor_invocation;
	};

	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	auto *mock_filesystem_ptr = mock_filesystem.get();
	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem));

	// Uncached read.
	{
		// Make sure it's mock file handle.
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto &cache_file_handle = handle->Cast<CacheFileSystemHandle>();
		auto &mock_file_handle = cache_file_handle.internal_file_handle->Cast<MockFileHandle>();

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

		// Glob operation.
		REQUIRE(cache_filesystem->Glob(TEST_GLOB_NAME).empty());
	}

	// Cache read.
	{
		// Make sure it's mock file handle.
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto &cache_file_handle = handle->Cast<CacheFileSystemHandle>();
		[[maybe_unused]] auto &mock_file_handle = cache_file_handle.internal_file_handle->Cast<MockFileHandle>();

		// Create a new handle, which cannot leverage the cached one.
		auto another_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		[[maybe_unused]] auto &another_mock_handle =
		    another_handle->Cast<CacheFileSystemHandle>().internal_file_handle->Cast<MockFileHandle>();

		std::string buffer(TEST_FILESIZE, '\0');
		cache_filesystem->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, /*location=*/0);
		REQUIRE(buffer == std::string(TEST_FILESIZE, 'a'));

		mock_filesystem_ptr->ClearReadOperations();
		auto read_operations = mock_filesystem_ptr->GetSortedReadOperations();
		REQUIRE(read_operations.empty());

		// Glob operation.
		REQUIRE(cache_filesystem->Glob(TEST_GLOB_NAME).empty());

		// [handle] and [another_handle] go out of scope and place back to file handle, but due to insufficient capacity
		// only one of them will be cached and another one closed and destructed.
	}

	// One of the file handles resides in cache, another one gets closed and destructed.
	REQUIRE(close_invocation == 1);
	REQUIRE(dtor_invocation == 1);
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 1);

	// Destructing the cache filesystem cleans file handle cache, which in turns close and destruct all cached file
	// handles.
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 2);

	cache_filesystem = nullptr;
	REQUIRE(close_invocation == 2);
	REQUIRE(dtor_invocation == 2);
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

TEST_CASE("Test clear cache", "[mock filesystem test]") {
	g_max_file_handle_cache_entry = 1;

	uint64_t close_invocation = 0;
	uint64_t dtor_invocation = 0;
	auto close_callback = [&close_invocation]() {
		++close_invocation;
	};
	auto dtor_callback = [&dtor_invocation]() {
		++dtor_invocation;
	};

	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	auto *mock_filesystem_ptr = mock_filesystem.get();
	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem));

	auto perform_io_operation = [&]() {
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		REQUIRE(cache_filesystem->Glob(TEST_GLOB_NAME).empty());
	};

	// Uncached IO operations.
	perform_io_operation();
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 1);
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 1);

	// Clear cache and perform IO operations.
	cache_filesystem->ClearCache();
	perform_io_operation();
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 2);
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 2);

	// Clear cache by filepath and perform IO operation.
	cache_filesystem->ClearCache(TEST_FILENAME);
	cache_filesystem->ClearCache(TEST_GLOB_NAME);
	perform_io_operation();
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 3);
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 3);

	// Retry one cached IO operation.
	perform_io_operation();
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 3);
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 3);
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

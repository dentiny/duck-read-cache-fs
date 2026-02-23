// Similar to on-disk reader unit test, this unit test also checks in-memory cache reader; but we write large file so
// threading issues and memory issues are easier to detect. Migrated from unit/.

#include "catch/catch.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "scope_guard.hpp"
#include "test_utils.hpp"

#include <utime.h>

using namespace duckdb; // NOLINT

namespace {

constexpr uint64_t TEST_ALPHA_ITER = 10000;
constexpr uint64_t LARGE_TEST_FILE_SIZE = 26 * TEST_ALPHA_ITER; // 260K
const auto LARGE_TEST_FILE_CONTENT = []() {
	string content(LARGE_TEST_FILE_SIZE, '\0');
	for (uint64_t ii = 0; ii < TEST_ALPHA_ITER; ++ii) {
		for (uint64_t jj = 0; jj < 26; ++jj) {
			const uint64_t idx = ii * 26 + jj;
			content[idx] = 'a' + jj;
		}
	}
	return content;
}();
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_httpfs_cache";

struct LargeFileInmemReaderFixture {
	string test_filename;
	LargeFileInmemReaderFixture() {
		test_filename = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
		auto local_filesystem = LocalFileSystem::CreateLocal();
		auto file_handle = local_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle,
		                        const_cast<void *>(static_cast<const void *>(LARGE_TEST_FILE_CONTENT.data())),
		                        LARGE_TEST_FILE_SIZE, /*location=*/0);
		file_handle->Sync();
		file_handle->Close();
	}
	~LargeFileInmemReaderFixture() {
		LocalFileSystem::CreateLocal()->RemoveFile(test_filename);
	}
};

} // namespace

TEST_CASE_METHOD(LargeFileInmemReaderFixture, "Read all bytes in one read operation",
                 "[in-memory cache filesystem test]") {
	constexpr uint64_t test_block_size = 22; // Intentionally not a divisor of file size.

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "in_mem";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(std::move(config));
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = LARGE_TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == LARGE_TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second cached read.
	{
		auto handle = disk_cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = LARGE_TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == LARGE_TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

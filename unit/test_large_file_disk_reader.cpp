// Similar to on-disk reader unit test, this unit test also checks disk cache reader; but we write large file so
// threading issues and memory issues are easier to detect.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "test_utils.hpp"

#include <utime.h>

using namespace duckdb; // NOLINT

namespace {

constexpr uint64_t TEST_ALPHA_ITER = 10000;
constexpr uint64_t TEST_FILE_SIZE = 26 * TEST_ALPHA_ITER; // 260K
const auto TEST_FILE_CONTENT = []() {
	string content(TEST_FILE_SIZE, '\0');
	for (uint64_t ii = 0; ii < TEST_ALPHA_ITER; ++ii) {
		for (uint64_t jj = 0; jj < 26; ++jj) {
			const uint64_t idx = ii * 26 + jj;
			content[idx] = 'a' + jj;
		}
	}
	return content;
}();
const auto TEST_FILENAME = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_httpfs_cache";
} // namespace

TEST_CASE("Read all bytes in one read operation", "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 22; // Intentionally not a divisor of file size.

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second cached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

int main(int argc, char **argv) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();

	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveFile(TEST_FILENAME);
	return result;
}

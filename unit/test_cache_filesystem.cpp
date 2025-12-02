#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

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
constexpr uint64_t TEST_FILE_SIZE = 26;
const auto TEST_FILE_CONTENT = []() {
	string content(TEST_FILE_SIZE, '\0');
	for (uint64_t idx = 0; idx < TEST_FILE_SIZE; ++idx) {
		content[idx] = 'a' + idx;
	}
	return content;
}();
const auto TEST_DIRECTORY = "/tmp/duckdb_test_cache";
const auto TEST_FILENAME = StringUtil::Format("/tmp/duckdb_test_cache/%s", UUID::ToString(UUID::GenerateRandomUUID()));

void PerformIoOperation(CacheFileSystem *cache_filesystem) {
	// Perform glob operation.
	auto open_file_info = cache_filesystem->Glob(TEST_FILENAME);
	REQUIRE(open_file_info.size() == 1);
	REQUIRE(open_file_info[0].path == TEST_FILENAME);
	// Perform open file operation.
	auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	// Perform get file size operation.
	REQUIRE(cache_filesystem->GetFileSize(*file_handle) == TEST_FILE_SIZE);
}

} // namespace

TEST_CASE("Test glob operation", "[cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.enable_glob_cache = true;
	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// Glob by filename.
	{
		auto open_file_info = cache_filesystem->Glob(TEST_FILENAME);
		REQUIRE(open_file_info.size() == 1);
		REQUIRE(open_file_info[0].path == TEST_FILENAME);
	}
	// Glob by pattern.
	{
		auto open_file_info = cache_filesystem->Glob("/tmp/duckdb_test_cache/*");
		REQUIRE(open_file_info.size() == 1);
		REQUIRE(open_file_info[0].path == TEST_FILENAME);
	}
}

TEST_CASE("Test clear cache", "[cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// Perform a series of IO operations without cache.
	PerformIoOperation(cache_filesystem);

	// Clear all cache and perform the same operation again.
	cache_filesystem->ClearCache();
	PerformIoOperation(cache_filesystem);

	// Clear cache via filepath and retry the same operations.
	cache_filesystem->ClearCache(TEST_FILENAME);
	PerformIoOperation(cache_filesystem);

	// Retry the same IO operations again.
	PerformIoOperation(cache_filesystem);
}

int main(int argc, char **argv) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	local_filesystem->CreateDirectory(TEST_DIRECTORY);
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();

	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveDirectory(TEST_DIRECTORY);
	return result;
}

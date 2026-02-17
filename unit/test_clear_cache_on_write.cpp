#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "scoped_directory.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {
const auto TEST_DIRECTORY = "/tmp/duckdb_test_cache_write";
const auto TEST_FILENAME =
    StringUtil::Format("/tmp/duckdb_test_cache_write/%s", UUID::ToString(UUID::GenerateRandomUUID()));
} // namespace

TEST_CASE("Test cache cleared on Write with location", "[cache filesystem write test]") {
	ScopedDirectory scoped_dir(TEST_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// Create test file
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();

	// Perform read operations to populate cache (metadata, file handle, glob)
	auto read_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	[[maybe_unused]] const int64_t original_size = cache_filesystem->GetFileSize(*read_handle);
	read_handle->Close();

	// Write to the file, this should clear all cache entries for this file
	const string new_content = "new content after write";
	auto write_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE);
	cache_filesystem->Write(*write_handle, const_cast<void *>(static_cast<const void *>(new_content.data())),
	                        new_content.length(), /*location=*/0);
	write_handle->Close();

	// Verify cache was cleared by reading metadata again, it should reflect the new file size
	auto verify_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	const int64_t new_size = cache_filesystem->GetFileSize(*verify_handle);
	REQUIRE(new_size == NumericCast<int64_t>(new_content.length()));
	verify_handle->Close();
}

TEST_CASE("Test cache cleared on Write without location", "[cache filesystem write test]") {
	ScopedDirectory scoped_dir(TEST_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// Create test file
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();

	// Perform read operations to populate cache
	auto read_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	[[maybe_unused]] const int64_t original_size = cache_filesystem->GetFileSize(*read_handle);
	read_handle->Close();

	// Write to the file using Write without location parameter, this should clear cache
	const string new_content = "different content";
	auto write_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE);
	const int64_t bytes_written = cache_filesystem->Write(
	    *write_handle, const_cast<void *>(static_cast<const void *>(new_content.data())), new_content.length());
	REQUIRE(bytes_written == NumericCast<int64_t>(new_content.length()));
	write_handle->Close();

	// Verify cache was cleared by reading metadata, should reflect new file size
	auto verify_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	const int64_t new_size = cache_filesystem->GetFileSize(*verify_handle);
	REQUIRE(new_size == NumericCast<int64_t>(new_content.length()));
	verify_handle->Close();
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

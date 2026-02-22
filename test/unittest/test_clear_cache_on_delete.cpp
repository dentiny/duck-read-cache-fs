#include "catch/catch.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "scoped_directory.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {
const auto TEST_DIRECTORY = "/tmp/duckdb_test_cache_delete";
const auto TEST_FILENAME =
    StringUtil::Format("/tmp/duckdb_test_cache_delete/%s", UUID::ToString(UUID::GenerateRandomUUID()));
} // namespace

TEST_CASE("Test cache cleared on RemoveFile", "[cache filesystem delete test]") {
	ScopedDirectory scoped_dir(TEST_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	// Intentionally not clearing cache on write to test cache invalidation on delete.
	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();
	auto local_filesystem = LocalFileSystem::CreateLocal();

	// Create the original file and populate cache via a read.
	auto create_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                   FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*create_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	create_handle->Sync();
	create_handle->Close();

	auto read_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	const int64_t original_size = cache_filesystem->GetFileSize(*read_handle);
	REQUIRE(original_size == TEST_FILE_SIZE);
	read_handle->Close();

	// Delete the file through the cache filesystem.
	cache_filesystem->RemoveFile(TEST_FILENAME);

	// Re-create the file with different content.
	const string new_content = "short";
	auto recreate_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                     FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*recreate_handle, const_cast<void *>(static_cast<const void *>(new_content.data())),
	                        new_content.length(), /*location=*/0);
	recreate_handle->Sync();
	recreate_handle->Close();

	// Check file size.
	auto verify_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	const int64_t new_size = cache_filesystem->GetFileSize(*verify_handle);
	REQUIRE(new_size == static_cast<int64_t>(new_content.length()));
	verify_handle->Close();
}

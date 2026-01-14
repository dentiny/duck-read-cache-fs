// Unit test for clear cache on write policy.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

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
const auto TEST_DIRECTORY = "/tmp/duckdb_test_cache_write_policy";
const auto CACHE_DIRECTORY = "/tmp/duckdb_test_cache_write_policy_cache";
const auto TEST_FILENAME =
    StringUtil::Format("/tmp/duckdb_test_cache_write_policy/%s", UUID::ToString(UUID::GenerateRandomUUID()));

// Test content strings
const string OLD_CONTENT = "original content for testing";
const string NEW_CONTENT = "new content after write";

// Util function to create and populate a test file with cache
void CreateAndCacheFile(CacheFileSystem *cache_filesystem, const string &filename, const string &content) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	{
		auto file_handle = local_filesystem->OpenFile(filename, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                            FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(content.data())),
		                        content.size(), /*location=*/0);
		file_handle->Sync();
		file_handle->Close();
	}

	// Perform read operations to populate cache
	{
		auto read_handle = cache_filesystem->OpenFile(filename, FileOpenFlags::FILE_FLAGS_READ);
		vector<char> buffer(content.size());
		cache_filesystem->Read(*read_handle, buffer.data(), buffer.size(), /*location=*/0);
		read_handle->Close();
	}
}
} // namespace

TEST_CASE("Test cache files NOT deleted when clear_cache_on_write_policy is 'disable'",
          "[cache clear on write policy]") {
	ScopedDirectory scoped_test_dir(TEST_DIRECTORY);
	ScopedDirectory scoped_cache_dir(CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.cache_directories = {CACHE_DIRECTORY};
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	config.clear_cache_on_write_option = *DISABLE_CLEAR_CACHE_ON_WRITE;

	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();
	CreateAndCacheFile(cache_filesystem, TEST_FILENAME, OLD_CONTENT);
	// Get cache count before write
	int cache_count_before = GetFileCountUnder(CACHE_DIRECTORY);
	REQUIRE(cache_count_before > 0);

	// Write to the file - cache should NOT be cleared in disable mode
	{
		auto write_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE);
		cache_filesystem->Write(*write_handle, const_cast<void *>(static_cast<const void *>(NEW_CONTENT.data())),
		                        NEW_CONTENT.length(), /*location=*/0);
		write_handle->Sync();
		write_handle->Close();
	}

	// Verify cache files still exist
	const int cache_count_after = GetFileCountUnder(CACHE_DIRECTORY);
	REQUIRE(cache_count_after == cache_count_before);
}

TEST_CASE("Test cache files deleted when clear_cache_on_write_policy is 'clear_for_cur_db'",
          "[cache clear on write policy]") {
	ScopedDirectory scoped_test_dir(TEST_DIRECTORY);
	ScopedDirectory scoped_cache_dir(CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.cache_directories = {CACHE_DIRECTORY};
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	config.clear_cache_on_write_option = *CLEAR_CACHE_ON_WRITE_CUR_DB;

	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();
	auto &instance_state = helper.GetInstanceStateOrThrow();

	// Mimic callback for option change setting
	instance_state.SetCacheFiles();
	CreateAndCacheFile(cache_filesystem, TEST_FILENAME, OLD_CONTENT);

	// Write to the file, which clears cache
	{
		auto write_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE);
		cache_filesystem->Write(*write_handle, const_cast<void *>(static_cast<const void *>(NEW_CONTENT.data())),
		                        NEW_CONTENT.length(), /*location=*/0);
		write_handle->Sync();
		write_handle->Close();
	}

	// Verify cache files were deleted
	int cache_count_after = GetFileCountUnder(CACHE_DIRECTORY);
	REQUIRE(cache_count_after == 0);
}

TEST_CASE("Test cache files deleted when clear_cache_on_write_policy is 'clear_cache_consistent'",
          "[cache clear on write policy]") {
	ScopedDirectory scoped_test_dir(TEST_DIRECTORY);
	ScopedDirectory scoped_cache_dir(CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.cache_directories = {CACHE_DIRECTORY};
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	config.clear_cache_on_write_option = *CLEAR_CACHE_ON_WRITE;

	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// Create and cache a test file
	CreateAndCacheFile(cache_filesystem, TEST_FILENAME, OLD_CONTENT);

	// Verify cache files exist
	const int cache_count_before = GetFileCountUnder(CACHE_DIRECTORY);
	REQUIRE(cache_count_before > 0);

	// Write to the file, cache should be cleared in clear_cache_consistent mode
	{
		auto write_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE);
		cache_filesystem->Write(*write_handle, const_cast<void *>(static_cast<const void *>(NEW_CONTENT.data())),
		                        NEW_CONTENT.length(), /*location=*/0);
		write_handle->Sync();
		write_handle->Close();
	}

	// Verify cache files were deleted
	const int cache_count_after = GetFileCountUnder(CACHE_DIRECTORY);
	REQUIRE(cache_count_after == 0);
}

TEST_CASE("Test cache directory change with disable policy does not clear old cache", "[cache clear on write policy]") {
	ScopedDirectory scoped_test_dir(TEST_DIRECTORY);
	ScopedDirectory scoped_cache_dir(CACHE_DIRECTORY);
	const string SECOND_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_write_policy_cache_2";
	ScopedDirectory scoped_cache_dir2(SECOND_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.cache_directories = {CACHE_DIRECTORY};
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	config.clear_cache_on_write_option = *DISABLE_CLEAR_CACHE_ON_WRITE;

	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();
	auto &inst_config = helper.GetConfig();

	// Create and cache a test file in the first cache directory
	CreateAndCacheFile(cache_filesystem, TEST_FILENAME, OLD_CONTENT);
	const int first_cache_count = GetFileCountUnder(CACHE_DIRECTORY);
	REQUIRE(first_cache_count > 0);
	REQUIRE(GetFileCountUnder(SECOND_CACHE_DIRECTORY) == 0);

	// Change cache directory
	inst_config.on_disk_cache_directories = {SECOND_CACHE_DIRECTORY};

	// Create a new cache filesystem with the updated config
	auto cache_fs2 = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal(), helper.GetInstanceStateSharedPtr());

	// Perform operations with the new cache directory
	const string TEST_FILENAME2 =
	    StringUtil::Format("/tmp/duckdb_test_cache_write_policy/%s", UUID::ToString(UUID::GenerateRandomUUID()));
	CreateAndCacheFile(cache_fs2.get(), TEST_FILENAME2, OLD_CONTENT);

	// Verify old cache files still exist in the first directory
	REQUIRE(GetFileCountUnder(CACHE_DIRECTORY) == first_cache_count);
	// Verify new cache files were created in the second directory
	REQUIRE(GetFileCountUnder(SECOND_CACHE_DIRECTORY) > 0);
}

TEST_CASE("Test switching clear_cache_on_write_policy dynamically", "[cache clear on write policy]") {
	ScopedDirectory scoped_test_dir(TEST_DIRECTORY);
	ScopedDirectory scoped_cache_dir(CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.cache_directories = {CACHE_DIRECTORY};
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	config.clear_cache_on_write_option = *DISABLE_CLEAR_CACHE_ON_WRITE;

	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();
	auto &inst_config = helper.GetConfig();
	auto &instance_state = helper.GetInstanceStateOrThrow();
	REQUIRE(inst_config.clear_cache_on_write_option == *DISABLE_CLEAR_CACHE_ON_WRITE);

	// Create and cache a test file
	CreateAndCacheFile(cache_filesystem, TEST_FILENAME, OLD_CONTENT);
	int cache_count_initial = GetFileCountUnder(CACHE_DIRECTORY);
	REQUIRE(cache_count_initial > 0);

	// Write with disable mode, cache should NOT be cleared
	{
		auto write_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE);
		cache_filesystem->Write(*write_handle, const_cast<void *>(static_cast<const void *>(NEW_CONTENT.data())),
		                        NEW_CONTENT.length(), /*location=*/0);
		write_handle->Close();
	}
	REQUIRE(GetFileCountUnder(CACHE_DIRECTORY) == cache_count_initial);

	// Switch to clear_for_cur_db mode
	inst_config.clear_cache_on_write_option = *CLEAR_CACHE_ON_WRITE_CUR_DB;
	instance_state.SetCacheFiles();

	// Re-populate cache
	CreateAndCacheFile(cache_filesystem, TEST_FILENAME, OLD_CONTENT);
	REQUIRE(GetFileCountUnder(CACHE_DIRECTORY) > 0);

	// Write with clear_for_cur_db mode, which clears cache
	{
		auto write_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE);
		cache_filesystem->Write(*write_handle, const_cast<void *>(static_cast<const void *>(NEW_CONTENT.data())),
		                        NEW_CONTENT.length(), /*location=*/0);
		write_handle->Close();
	}
	REQUIRE(GetFileCountUnder(CACHE_DIRECTORY) == 0);
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

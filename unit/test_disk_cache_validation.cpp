#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"
#include "filesystem_utils.hpp"
#include "scope_guard.hpp"
#include "test_utils.hpp"

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
const auto TEST_FILENAME = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_httpfs_cache";

void CreateSourceTestFile() {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();
}

// A filesystem wrapper that extends LocalFileSystem but allows setting version tags
class VersionTagFileSystem : public LocalFileSystem {
public:
	void SetVersionTag(const string &tag) {
		version_tag = tag;
	}
	string GetVersionTag(FileHandle &handle) override {
		return version_tag;
	}

private:
	string version_tag;
};

// Helper struct to create a CacheFileSystem with a custom internal filesystem
struct ValidationTestHelper {
	DuckDB db;
	shared_ptr<CacheHttpfsInstanceState> instance_state;
	unique_ptr<CacheFileSystem> cache_fs;
	VersionTagFileSystem *version_tag_fs_ptr = nullptr; // Non-owning pointer for setting version tag

	ValidationTestHelper(idx_t block_size, bool enable_validation, bool enable_disk_reader_mem_cache = false) {
		instance_state = make_shared_ptr<CacheHttpfsInstanceState>();

		// Configure the instance
		auto &config = instance_state->config;
		config.cache_type = *ON_DISK_CACHE_TYPE;
		config.cache_block_size = block_size;
		config.on_disk_cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
		config.enable_cache_validation = enable_validation;
		config.enable_disk_reader_mem_cache = enable_disk_reader_mem_cache;

		// Ensure cache directories exist
		auto local_fs = LocalFileSystem::CreateLocal();
		local_fs->CreateDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

		// Register state with instance
		SetInstanceState(*db.instance.get(), instance_state);
		InitializeCacheReaderForTest(instance_state, config);

		// Create cache filesystem wrapping version tag filesystem
		auto version_tag_fs = make_uniq<VersionTagFileSystem>();
		version_tag_fs_ptr = version_tag_fs.get();
		cache_fs = make_uniq<CacheFileSystem>(std::move(version_tag_fs), instance_state);
	}

	void SetVersionTag(const string &tag) {
		version_tag_fs_ptr->SetVersionTag(tag);
	}

	CacheFileSystem *GetCacheFileSystem() {
		return cache_fs.get();
	}
};
} // namespace

// Testing scenario: cache validation with matching version tags should use cached file.
TEST_CASE("Test disk cache validation with matching version tags", "[disk cache validation test]") {
	constexpr uint64_t test_block_size = 10;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	ValidationTestHelper helper(test_block_size, /*enable_validation=*/true);
	const string test_version_tag = "version-1.0";
	helper.SetVersionTag(test_version_tag);
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// First read: should cache the file with version tag
	{
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		string content(test_block_size, '\0');
		cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), test_block_size,
		                       /*location=*/0);
		REQUIRE(content == TEST_FILE_CONTENT.substr(0, test_block_size));
	}

	// Verify cache file was created and has version tag stored
	auto cache_files = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(cache_files.size() == 1);
	string cache_file_path = StringUtil::Format("%s/%s", TEST_ON_DISK_CACHE_DIRECTORY, cache_files[0]);
	string cached_version_tag = GetCacheVersion(cache_file_path);
	REQUIRE(cached_version_tag == test_version_tag);

	// Second read with same version tag: should use cache
	{
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		string content(test_block_size, '\0');
		cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), test_block_size,
		                       /*location=*/0);
		REQUIRE(content == TEST_FILE_CONTENT.substr(0, test_block_size));
	}
}

// Testing scenario: cache validation with mismatched version tags should invalidate cache.
TEST_CASE("Test disk cache validation with mismatched version tags", "[disk cache validation test]") {
	constexpr uint64_t test_block_size = 10;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	ValidationTestHelper helper(test_block_size, /*enable_validation=*/true);
	const string initial_version_tag = "version-1.0";
	helper.SetVersionTag(initial_version_tag);
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// First read: cache the file
	{
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		string content(test_block_size, '\0');
		cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), test_block_size,
		                       /*location=*/0);
		REQUIRE(content == TEST_FILE_CONTENT.substr(0, test_block_size));
	}

	// Verify cache file exists and has initial version tag
	auto cache_files = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(cache_files.size() == 1);
	string cache_file_path = StringUtil::Format("%s/%s", TEST_ON_DISK_CACHE_DIRECTORY, cache_files[0]);
	string cached_version_tag = GetCacheVersion(cache_file_path);
	REQUIRE(cached_version_tag == initial_version_tag);

	// Create a new helper with different version tag to simulate file update on remote
	ValidationTestHelper new_helper(test_block_size, /*enable_validation=*/true);
	const string new_version_tag = "version-2.0";
	new_helper.SetVersionTag(new_version_tag);
	auto *new_cache_filesystem = new_helper.GetCacheFileSystem();

	// Second read with different version tag: should invalidate cache and re-fetch
	{
		auto handle = new_cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		string content(test_block_size, '\0');
		new_cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
		                           test_block_size, /*location=*/0);
		REQUIRE(content == TEST_FILE_CONTENT.substr(0, test_block_size));
	}

	// New cache file should have new version tag
	cache_files = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(cache_files.size() == 1);
	cache_file_path = StringUtil::Format("%s/%s", TEST_ON_DISK_CACHE_DIRECTORY, cache_files[0]);
	cached_version_tag = GetCacheVersion(cache_file_path);
	REQUIRE(cached_version_tag == new_version_tag);
}

// Testing scenario: cache file without version tag should be invalidated when validation is enabled.
TEST_CASE("Test disk cache validation with missing version tag in cache file", "[disk cache validation test]") {
	constexpr uint64_t test_block_size = 10;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	// Create cache file without version tag (simulating old cache) by disabling validation
	ValidationTestHelper helper(test_block_size, /*enable_validation=*/false);
	helper.SetVersionTag(""); // Empty version tag means validation disabled
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// First read: cache the file without version tag
	{
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		string content(test_block_size, '\0');
		cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), test_block_size,
		                       /*location=*/0);
		REQUIRE(content == TEST_FILE_CONTENT.substr(0, test_block_size));
	}

	// Verify cache file exists but has no version tag
	auto cache_files = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(cache_files.size() == 1);
	string cache_file_path = StringUtil::Format("%s/%s", TEST_ON_DISK_CACHE_DIRECTORY, cache_files[0]);
	string cached_version_tag = GetCacheVersion(cache_file_path);
	REQUIRE(cached_version_tag.empty()); // No version tag stored

	// Now create a new helper with validation enabled and a version tag
	ValidationTestHelper new_helper(test_block_size, /*enable_validation=*/true);
	const string test_version_tag = "version-1.0";
	new_helper.SetVersionTag(test_version_tag);
	auto *new_cache_filesystem = new_helper.GetCacheFileSystem();

	// Read again: cache file without version tag should be invalidated and re-fetched
	{
		auto handle = new_cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		string content(test_block_size, '\0');
		new_cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
		                           test_block_size, /*location=*/0);
		REQUIRE(content == TEST_FILE_CONTENT.substr(0, test_block_size));
	}

	// Cache file should now have version tag
	cache_files = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(cache_files.size() == 1);
	cache_file_path = StringUtil::Format("%s/%s", TEST_ON_DISK_CACHE_DIRECTORY, cache_files[0]);
	cached_version_tag = GetCacheVersion(cache_file_path);
	REQUIRE(cached_version_tag == test_version_tag);
}

// Testing scenario: cache validation disabled should always use cache regardless of version tags.
TEST_CASE("Test disk cache with validation disabled", "[disk cache validation test]") {
	constexpr uint64_t test_block_size = 10;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	ValidationTestHelper helper(test_block_size, /*enable_validation=*/false);
	helper.SetVersionTag("version-1.0");
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// First read: cache the file
	{
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		string content(test_block_size, '\0');
		cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), test_block_size,
		                       /*location=*/0);
		REQUIRE(content == TEST_FILE_CONTENT.substr(0, test_block_size));
	}

	// Verify cache file exists (may or may not have version tag since validation is disabled)
	auto cache_files = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(cache_files.size() == 1);

	// Create a new helper with different version tag (should be ignored when validation is disabled)
	ValidationTestHelper new_helper(test_block_size, /*enable_validation=*/false);
	new_helper.SetVersionTag("version-2.0");
	auto *new_cache_filesystem = new_helper.GetCacheFileSystem();

	// Second read: should use cache even though version changed (validation disabled)
	{
		auto handle = new_cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		string content(test_block_size, '\0');
		new_cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
		                           test_block_size, /*location=*/0);
		REQUIRE(content == TEST_FILE_CONTENT.substr(0, test_block_size));
	}
}

int main(int argc, char **argv) {
	CreateSourceTestFile();

	int result = Catch::Session().run(argc, argv);
	LocalFileSystem::CreateLocal()->RemoveFile(TEST_FILENAME);
	return result;
}

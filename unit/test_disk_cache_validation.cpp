#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
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
} // namespace

// Testing scenario: cache validation with matching version tags should use cached file.
TEST_CASE("Test disk cache validation with matching version tags", "[disk cache validation test]") {
	constexpr uint64_t test_block_size = 10;
	*g_on_disk_cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	g_cache_block_size = test_block_size;
	g_enable_cache_validation = true;
	g_enable_disk_reader_mem_cache = false; // Disable in-memory cache to test disk cache directly
	SCOPE_EXIT {
		ResetGlobalStateAndConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	auto version_tag_fs = make_uniq<VersionTagFileSystem>();
	const string test_version_tag = "version-1.0";
	version_tag_fs->SetVersionTag(test_version_tag);
	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(version_tag_fs));

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
	*g_on_disk_cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	g_cache_block_size = test_block_size;
	g_enable_cache_validation = true;
	g_enable_disk_reader_mem_cache = false;
	SCOPE_EXIT {
		ResetGlobalStateAndConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	auto version_tag_fs = make_uniq<VersionTagFileSystem>();
	const string initial_version_tag = "version-1.0";
	version_tag_fs->SetVersionTag(initial_version_tag);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(version_tag_fs));

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

	// Change version tag to simulate file update on remote
	const string new_version_tag = "version-2.0";
	auto new_version_tag_fs = make_uniq<VersionTagFileSystem>();
	new_version_tag_fs->SetVersionTag(new_version_tag);
	auto new_cache_filesystem = make_uniq<CacheFileSystem>(std::move(new_version_tag_fs));

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
	*g_on_disk_cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	g_cache_block_size = test_block_size;
	g_enable_cache_validation = false; // First create cache without validation
	g_enable_disk_reader_mem_cache = false;
	SCOPE_EXIT {
		ResetGlobalStateAndConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	// Create cache file without version tag (simulating old cache)
	auto no_validation_fs = make_uniq<VersionTagFileSystem>();
	no_validation_fs->SetVersionTag(""); // Empty version tag means validation disabled
	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(no_validation_fs));

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

	// Now enable validation and use a filesystem with version tag
	g_enable_cache_validation = true;
	const string test_version_tag = "version-1.0";
	auto version_tag_fs = make_uniq<VersionTagFileSystem>();
	version_tag_fs->SetVersionTag(test_version_tag);
	auto new_cache_filesystem = make_uniq<CacheFileSystem>(std::move(version_tag_fs));

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
	*g_on_disk_cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	g_cache_block_size = test_block_size;
	g_enable_cache_validation = false; // Disable validation
	g_enable_disk_reader_mem_cache = false;
	SCOPE_EXIT {
		ResetGlobalStateAndConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	auto version_tag_fs = make_uniq<VersionTagFileSystem>();
	version_tag_fs->SetVersionTag("version-1.0");

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(version_tag_fs));

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

	// Change version tag (should be ignored when validation is disabled)
	auto new_version_tag_fs = make_uniq<VersionTagFileSystem>();
	new_version_tag_fs->SetVersionTag("version-2.0");
	auto new_cache_filesystem = make_uniq<CacheFileSystem>(std::move(new_version_tag_fs));

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
	// Set global cache type for testing.
	*g_test_cache_type = *ON_DISK_CACHE_TYPE;
	CreateSourceTestFile();

	int result = Catch::Session().run(argc, argv);
	LocalFileSystem::CreateLocal()->RemoveFile(TEST_FILENAME);
	return result;
}

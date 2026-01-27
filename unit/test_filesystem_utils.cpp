// This file tests the stale file deletion.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string_util.hpp"
#include "cache_filesystem_config.hpp"
#include "filesystem_utils.hpp"
#include "hffs.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/main/database.hpp"

#include <utime.h>

using namespace duckdb; // NOLINT

namespace {
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_httpfs_cache";
} // namespace

TEST_CASE("DEBUG HTTPFS FILESYSTEM", "[utils test]") {
	auto fs = make_uniq<HTTPFileSystem>();
	// auto database_instance = make_shared_ptr<DatabaseInstance>();
	// auto& buffer_manager = database_instance->GetBufferManager();
	// auto buffer = buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, 33554428);
	// QueryContext context;
	std::cerr << fs->FileExists("https://github.com/duckdb/duckdb-data/releases/download/v1.0/example_rn.ndjson", nullptr) << std::endl;
}

TEST_CASE("Stale file deletion", "[utils test]") {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	const string fname1 = StringUtil::Format("%s/file1", TEST_ON_DISK_CACHE_DIRECTORY);
	const string fname2 = StringUtil::Format("%s/file2", TEST_ON_DISK_CACHE_DIRECTORY);
	const std::string CONTENT = "helloworld";

	{
		auto file_handle = local_filesystem->OpenFile(fname1, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                          FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	}
	{
		auto file_handle = local_filesystem->OpenFile(fname2, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                          FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	}

	const time_t now = std::time(nullptr);
	const time_t two_day_ago = now - 48 * 60 * 60;
	struct utimbuf updated_time;
	updated_time.actime = two_day_ago;
	updated_time.modtime = two_day_ago;
	REQUIRE(utime(fname2.data(), &updated_time) == 0);

	// Get files under the folder in the order of creation timestamp.
	{
		const vector<string> folders {TEST_ON_DISK_CACHE_DIRECTORY};
		const auto files = GetOnDiskFilesUnder(folders);
		REQUIRE(files.size() == 2);
		REQUIRE(files.begin()->second == fname2);
		REQUIRE(files.rbegin()->second == fname1);
	}

	// Evict stale cache files.
	EvictStaleCacheFiles(*local_filesystem, TEST_ON_DISK_CACHE_DIRECTORY);
	vector<string> fresh_files;
	REQUIRE(
	    local_filesystem->ListFiles(TEST_ON_DISK_CACHE_DIRECTORY, [&fresh_files](const string &fname, bool /*unused*/) {
		    fresh_files.emplace_back(StringUtil::Format("%s/%s", TEST_ON_DISK_CACHE_DIRECTORY, fname));
	    }));
	REQUIRE(fresh_files == vector<string> {fname1});
}

TEST_CASE("Cache version roundtrip", "[utils test]") {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	const string test_file = StringUtil::Format("%s/version_test_file", TEST_ON_DISK_CACHE_DIRECTORY);
	const string test_version = "v1.2.3-test-version";

	// Create a test file
	{
		auto file_handle = local_filesystem->OpenFile(test_file, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                             FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>("test content")), 12,
		                        /*location=*/0);
		file_handle->Sync();
		file_handle->Close();
	}

	// Test roundtrip: set and get
	REQUIRE(SetCacheVersion(test_file, test_version) == true);
	const string retrieved_version = GetCacheVersion(test_file);
	REQUIRE(retrieved_version == test_version);

	// Test updating the version
	const string updated_version = "v2.0.0-updated";
	REQUIRE(SetCacheVersion(test_file, updated_version) == true);
	const string retrieved_updated_version = GetCacheVersion(test_file);
	REQUIRE(retrieved_updated_version == updated_version);

	// Test getting version from non-existent file
	const string non_existent_file = StringUtil::Format("%s/non_existent_file", TEST_ON_DISK_CACHE_DIRECTORY);
	const string empty_version = GetCacheVersion(non_existent_file);
	REQUIRE(empty_version.empty());

	// Test cleanup.
	local_filesystem->RemoveFile(test_file);
}

int main(int argc, char **argv) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	local_filesystem->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	local_filesystem->CreateDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	return result;
}

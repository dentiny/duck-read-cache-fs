// This file tests the stale file deletion.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string_util.hpp"
#include "cache_filesystem_config.hpp"
#include "filesystem_utils.hpp"
#include "scoped_directory.hpp"

#include <utime.h>

using namespace duckdb; // NOLINT

namespace {
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_httpfs_cache";
} // namespace

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

TEST_CASE("SetFileAttributes and GetFileAttribute roundtrip", "[utils test]") {
	ScopedDirectory scoped_dir("/tmp/duckdb_test_attr_roundtrip");
	const auto &dir = scoped_dir.GetPath();

	auto local_filesystem = LocalFileSystem::CreateLocal();
	const string test_file = StringUtil::Format("%s/attr_roundtrip_file", dir);

	// Create a test file.
	{
		auto fh = local_filesystem->OpenFile(test_file, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                    FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*fh, const_cast<void *>(static_cast<const void *>("payload")), 7, /*location=*/0);
		fh->Sync();
		fh->Close();
	}

	// Set multiple attributes and read them back individually.
	unordered_map<string, string> attrs;
	attrs["user.test_key_a"] = "value_alpha";
	attrs["user.test_key_b"] = "value_beta";
	REQUIRE(SetFileAttributes(test_file, attrs));

	REQUIRE(GetFileAttribute(test_file, "user.test_key_a") == "value_alpha");
	REQUIRE(GetFileAttribute(test_file, "user.test_key_b") == "value_beta");

	// Overwrite one attribute and verify.
	unordered_map<string, string> updated;
	updated["user.test_key_a"] = "value_updated";
	REQUIRE(SetFileAttributes(test_file, updated));
	REQUIRE(GetFileAttribute(test_file, "user.test_key_a") == "value_updated");
	// The other attribute should be untouched.
	REQUIRE(GetFileAttribute(test_file, "user.test_key_b") == "value_beta");

	// Reading a non-existent attribute returns empty.
	REQUIRE(GetFileAttribute(test_file, "user.no_such_key").empty());

	// Reading from a non-existent file returns empty.
	REQUIRE(GetFileAttribute(StringUtil::Format("%s/no_such_file", dir), "user.test_key_a").empty());
}

int main(int argc, char **argv) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	local_filesystem->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	local_filesystem->CreateDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	return result;
}

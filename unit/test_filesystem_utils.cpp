// This file tests the stale file deletion.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "cache_filesystem_config.hpp"
#include "filesystem_utils.hpp"

#include <utime.h>

#include <iostream>

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

	EvictStaleCacheFiles(*local_filesystem, TEST_ON_DISK_CACHE_DIRECTORY);
	vector<string> fresh_files;
	REQUIRE(
	    local_filesystem->ListFiles(TEST_ON_DISK_CACHE_DIRECTORY, [&fresh_files](const string &fname, bool /*unused*/) {
		    fresh_files.emplace_back(StringUtil::Format("%s/%s", TEST_ON_DISK_CACHE_DIRECTORY, fname));
	    }));
	REQUIRE(fresh_files == vector<string> {fname1});
}

// TODO(hjiang): Here we simply manually check the output, but it's not a good unit test, in theory we should mimic
// gtest and capture the output of `df` and compare. Reference:
// -
// https://github.com/google/googletest/blob/e88cb95b92acbdce9b058dd894a68e1281b38495/googletest/include/gtest/internal/gtest-port.h#L241-L247
// - df -h /tmp/duckdb_cache_httpfs_cache | awk 'NR==2 {print $2}'
TEST_CASE("Get total filesystem size", "[utils test]") {
	std::cout << GetOverallFileSystemDiskSpace(DEFAULT_ON_DISK_CACHE_DIRECTORY) << std::endl;
}

int main(int argc, char **argv) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	local_filesystem->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	local_filesystem->CreateDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	return result;
}

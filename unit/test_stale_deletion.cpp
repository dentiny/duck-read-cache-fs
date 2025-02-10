// This file tests the stale file deletion.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "filesystem_utils.hpp"
#include "duckdb/common/string_util.hpp"

#include <utime.h>

using namespace duckdb; // NOLINT

namespace {
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cached_http_cache";
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
	const time_t one_day_ago = now - 24 * 60 * 60;
	struct utimbuf updated_time;
	updated_time.actime = one_day_ago;
	updated_time.modtime = one_day_ago;
	REQUIRE(utime(fname2.data(), &updated_time) == 0);

	EvictStaleCacheFiles(*local_filesystem, TEST_ON_DISK_CACHE_DIRECTORY);
	vector<string> fresh_files;
	REQUIRE(
	    local_filesystem->ListFiles(TEST_ON_DISK_CACHE_DIRECTORY, [&fresh_files](const string &fname, bool /*unused*/) {
		    fresh_files.emplace_back(StringUtil::Format("%s/%s", TEST_ON_DISK_CACHE_DIRECTORY, fname));
	    }));
	REQUIRE(fresh_files == vector<string> {fname1});
}

int main(int argc, char **argv) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	local_filesystem->CreateDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	return result;
}

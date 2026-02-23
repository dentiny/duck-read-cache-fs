#include "catch/catch.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "scope_guard.hpp"
#include "scoped_directory.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"

#include <utime.h>

using namespace duckdb; // NOLINT

namespace {

struct CacheFilesystemFixture {
	ScopedDirectory scoped_dir;
	string test_filename;
	CacheFilesystemFixture()
	    : scoped_dir(StringUtil::Format("/tmp/duckdb_test_cache_%s", UUID::ToString(UUID::GenerateRandomUUID()))) {
		test_filename = StringUtil::Format("%s/source_file", scoped_dir.GetPath());
		auto local_filesystem = LocalFileSystem::CreateLocal();
		auto file_handle = local_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
		                        TEST_FILE_SIZE, /*location=*/0);
		file_handle->Sync();
		file_handle->Close();
	}
};

void PerformIoOperation(CacheFileSystem *cache_filesystem, const string &test_filename) {
	// Perform glob operation.
	auto open_file_info = cache_filesystem->Glob(test_filename);
	REQUIRE(open_file_info.size() == 1);
	REQUIRE(open_file_info[0].path == test_filename);
	// Perform open file operation.
	auto file_handle = cache_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
	// Perform get file size operation.
	REQUIRE(cache_filesystem->GetFileSize(*file_handle) == TEST_FILE_SIZE);
}

} // namespace

TEST_CASE_METHOD(CacheFilesystemFixture, "Test glob operation", "[cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.enable_glob_cache = true;
	TestCacheFileSystemHelper helper(std::move(config));
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// Glob by filename.
	{
		auto open_file_info = cache_filesystem->Glob(test_filename);
		REQUIRE(open_file_info.size() == 1);
		REQUIRE(open_file_info[0].path == test_filename);
	}
	// Glob by pattern.
	{
		auto open_file_info = cache_filesystem->Glob(StringUtil::Format("%s/*", scoped_dir.GetPath()));
		REQUIRE(open_file_info.size() == 1);
		REQUIRE(open_file_info[0].path == test_filename);
	}
}

TEST_CASE_METHOD(CacheFilesystemFixture, "Test clear cache on disk cache filesystem", "[cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;
	TestCacheFileSystemHelper helper(std::move(config));
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// Perform a series of IO operations without cache.
	PerformIoOperation(cache_filesystem, test_filename);

	// Clear all cache and perform the same operation again.
	cache_filesystem->ClearCache();
	PerformIoOperation(cache_filesystem, test_filename);

	// Clear cache via filepath and retry the same operations.
	cache_filesystem->ClearCache(test_filename);
	PerformIoOperation(cache_filesystem, test_filename);

	// Retry the same IO operations again.
	PerformIoOperation(cache_filesystem, test_filename);
}

// Similar to on-disk reader unit test, this unit test checks situations where multiple cache directories are specified.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "scope_guard.hpp"

#include <utime.h>

using namespace duckdb; // NOLINT

namespace {
constexpr uint64_t TEST_FILE_SIZE = 26;
constexpr idx_t TEST_FILE_COUNT = 100;
const auto TEST_FILE_CONTENT = []() {
	string content(TEST_FILE_SIZE, '\0');
	for (uint64_t idx = 0; idx < TEST_FILE_SIZE; ++idx) {
		content[idx] = 'a' + idx;
	}
	return content;
}();
const auto TEST_FILES = []() {
	vector<string> test_files;
	test_files.reserve(TEST_FILE_COUNT);
	for (idx_t idx = 0; idx < TEST_FILE_COUNT; ++idx) {
		test_files.emplace_back(StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID())));
	}
	return test_files;
}();
const auto TEST_ON_DISK_CACHE_DIRECTORIES = []() {
	vector<string> cache_directories;
	cache_directories.reserve(TEST_FILE_COUNT);
	for (idx_t idx = 0; idx < TEST_FILE_COUNT; ++idx) {
		cache_directories.emplace_back(StringUtil::Format("/tmp/duckdb_test_cache_httpfs_cache_%d", idx));
	}
	return cache_directories;
}();
} // namespace

TEST_CASE("Test for cache directory config with multiple directories", "[on-disk cache filesystem test]") {
	g_cache_block_size = TEST_FILE_SIZE;
	*g_on_disk_cache_directories = TEST_ON_DISK_CACHE_DIRECTORIES;
	auto delete_cache_directories = []() {
		for (const auto &cur_cache_dir : *g_on_disk_cache_directories) {
			LocalFileSystem::CreateLocal()->RemoveDirectory(cur_cache_dir);
		}
	};

	SCOPE_EXIT {
		ResetGlobalConfig();
		delete_cache_directories();
	};

	delete_cache_directories();
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

	// First uncached read.
	{
		string content(TEST_FILE_SIZE, '\0');
		for (const auto &cur_file : TEST_FILES) {
			auto handle = disk_cache_fs->OpenFile(cur_file, FileOpenFlags::FILE_FLAGS_READ);
			disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
			                    /*nr_bytes=*/TEST_FILE_SIZE,
			                    /*location=*/0);
			REQUIRE(content == TEST_FILE_CONTENT);
		}
	}

	// Check more than one cache directories are not empty.
	vector<int> file_counts_first_read(TEST_FILE_COUNT, 0);
	int non_empty_directory_count = 0;
	for (idx_t idx = 0; idx < TEST_FILE_COUNT; ++idx) {
		const auto file_count = GetFileCountUnder((*g_on_disk_cache_directories)[idx]);
		file_counts_first_read[idx] = file_count;
		non_empty_directory_count += static_cast<int>(file_count > 0);
	}
	REQUIRE(non_empty_directory_count > 1);

	// Second cached read.
	{
		string content(TEST_FILE_SIZE, '\0');
		for (const auto &cur_file : TEST_FILES) {
			auto handle = disk_cache_fs->OpenFile(cur_file, FileOpenFlags::FILE_FLAGS_READ);
			disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
			                    /*nr_bytes=*/TEST_FILE_SIZE,
			                    /*location=*/0);
			REQUIRE(content == TEST_FILE_CONTENT);
		}
	}

	// Check second read has 100% cache hit so no cache files changed.
	vector<int> file_counts_second_read(TEST_FILE_COUNT, 0);
	for (idx_t idx = 0; idx < TEST_FILE_COUNT; ++idx) {
		const auto file_count = GetFileCountUnder((*g_on_disk_cache_directories)[idx]);
		file_counts_second_read[idx] = file_count;
	}
	REQUIRE(file_counts_first_read == file_counts_second_read);
}

int main(int argc, char **argv) {
	// Set global cache type for testing.
	*g_test_cache_type = *ON_DISK_CACHE_TYPE;

	// Create test files.
	auto local_filesystem = LocalFileSystem::CreateLocal();
	for (const auto &cur_file : TEST_FILES) {
		auto file_handle = local_filesystem->OpenFile(cur_file, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                            FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
		                        TEST_FILE_SIZE, /*location=*/0);
		file_handle->Sync();
		file_handle->Close();
	}

	int result = Catch::Session().run(argc, argv);

	// Delete test files.
	for (const auto &cur_file : TEST_FILES) {
		local_filesystem->RemoveFile(cur_file);
	}

	return result;
}

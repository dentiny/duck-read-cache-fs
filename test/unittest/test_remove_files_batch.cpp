#include "catch/catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "mock_filesystem.hpp"
#include "scoped_directory.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {
const auto TEST_DIRECTORY = "/tmp/duckdb_test_remove_files_batch";
} // namespace

TEST_CASE("Test RemoveFiles issues a single batched call to the internal filesystem",
          "[cache filesystem remove files batch]") {
	uint64_t close_invocation = 0;
	uint64_t dtor_invocation = 0;

	auto mock_filesystem = make_uniq<MockFileSystem>([&close_invocation]() { ++close_invocation; },
	                                                 [&dtor_invocation]() { ++dtor_invocation; });
	auto *mock_ptr = mock_filesystem.get();

	TestCacheConfig config;
	config.internal_filesystem = std::move(mock_filesystem);
	TestCacheFileSystemHelper helper(std::move(config));
	auto *cache_filesystem = helper.GetCacheFileSystem();

	const vector<string> paths = {"s3://bucket/file_a.parquet", "s3://bucket/file_b.parquet",
	                              "s3://bucket/file_c.parquet"};

	cache_filesystem->RemoveFiles(paths);

	REQUIRE(mock_ptr->GetRemoveFilesInvocation() == 1);

	const auto removed = mock_ptr->GetRemovedFiles();
	REQUIRE(removed.size() == paths.size());
	for (const auto &path : paths) {
		REQUIRE(std::find(removed.begin(), removed.end(), path) != removed.end());
	}
}

TEST_CASE("Test cache cleared for all files on RemoveFiles", "[cache filesystem remove files batch]") {
	ScopedDirectory scoped_dir(TEST_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.enable_glob_cache = true;
	config.enable_file_handle_cache = true;
	config.enable_metadata_cache = true;

	TestCacheFileSystemHelper helper(std::move(config));
	auto *cache_filesystem = helper.GetCacheFileSystem();
	auto local_filesystem = LocalFileSystem::CreateLocal();

	const string filename_a = StringUtil::Format("%s/%s_a", TEST_DIRECTORY, UUID::ToString(UUID::GenerateRandomUUID()));
	const string filename_b = StringUtil::Format("%s/%s_b", TEST_DIRECTORY, UUID::ToString(UUID::GenerateRandomUUID()));

	for (const auto &filename : {filename_a, filename_b}) {
		auto create_handle = local_filesystem->OpenFile(filename, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                              FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*create_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
		                        TEST_FILE_SIZE, /*location=*/0);
		create_handle->Sync();
		create_handle->Close();

		auto read_handle = cache_filesystem->OpenFile(filename, FileOpenFlags::FILE_FLAGS_READ);
		REQUIRE(cache_filesystem->GetFileSize(*read_handle) == TEST_FILE_SIZE);
		read_handle->Close();
	}

	cache_filesystem->RemoveFiles({filename_a, filename_b});

	const string new_content = "x";
	for (const auto &filename : {filename_a, filename_b}) {
		auto recreate_handle = local_filesystem->OpenFile(filename, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                                FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*recreate_handle, const_cast<void *>(static_cast<const void *>(new_content.data())),
		                        new_content.length(), /*location=*/0);
		recreate_handle->Sync();
		recreate_handle->Close();
	}

	for (const auto &filename : {filename_a, filename_b}) {
		auto verify_handle = cache_filesystem->OpenFile(filename, FileOpenFlags::FILE_FLAGS_READ);
		REQUIRE(cache_filesystem->GetFileSize(*verify_handle) == static_cast<int64_t>(new_content.length()));
		verify_handle->Close();
	}
}

// Unit test for multiple DuckDB instances in a single process. Migrated from unit/.

#include "catch/catch.hpp"

#include "cache_filesystem_config.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "scoped_directory.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

struct MultiDuckDBInstanceFixture {
	string test_filename;
	MultiDuckDBInstanceFixture() {
		test_filename = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
		auto local_filesystem = LocalFileSystem::CreateLocal();
		auto file_handle = local_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
		                        TEST_FILE_SIZE, /*location=*/0);
		file_handle->Sync();
		file_handle->Close();
	}
	~MultiDuckDBInstanceFixture() {
		LocalFileSystem::CreateLocal()->RemoveFile(test_filename);
	}
};

} // namespace

TEST_CASE_METHOD(MultiDuckDBInstanceFixture, "Test multiple instances with different cache types",
                 "[multi-instance test]") {
	const string cache_dir_1 = "/tmp/duckdb_multi_test_cache_1";
	const string cache_dir_2 = "/tmp/duckdb_multi_test_cache_2";
	const string cache_dir_3 = "/tmp/duckdb_multi_test_cache_3";

	ScopedDirectory scoped_dir_1(cache_dir_1);
	ScopedDirectory scoped_dir_2(cache_dir_2);
	ScopedDirectory scoped_dir_3(cache_dir_3);

	// Create first instance with on disk cache
	TestCacheConfig config1;
	config1.cache_type = "on_disk";
	config1.cache_block_size = 5;
	config1.cache_directories = {cache_dir_1};
	TestCacheFileSystemHelper helper1(std::move(config1));
	auto *cache_fs1 = helper1.GetCacheFileSystem();
	[[maybe_unused]] auto &inst_state1 = helper1.GetInstanceStateOrThrow();
	auto &config1_ref = helper1.GetConfig();

	// Create second instance with in_mem cache
	TestCacheConfig config2;
	config2.cache_type = "in_mem";
	config2.cache_block_size = 8;
	config2.cache_directories = {cache_dir_2};
	TestCacheFileSystemHelper helper2(std::move(config2));
	auto *cache_fs2 = helper2.GetCacheFileSystem();
	[[maybe_unused]] auto &inst_state2 = helper2.GetInstanceStateOrThrow();
	auto &config2_ref = helper2.GetConfig();

	// Create third instance with noop cache
	TestCacheConfig config3;
	config3.cache_type = "noop";
	config3.cache_block_size = 10;
	config3.cache_directories = {cache_dir_3};
	TestCacheFileSystemHelper helper3(std::move(config3));
	auto *cache_fs3 = helper3.GetCacheFileSystem();
	[[maybe_unused]] auto &inst_state3 = helper3.GetInstanceStateOrThrow();
	auto &config3_ref = helper3.GetConfig();

	// Verify each instance has its own independent configuration
	REQUIRE(config1_ref.cache_type == "on_disk");
	REQUIRE(config1_ref.cache_block_size == 5);
	REQUIRE(config1_ref.on_disk_cache_directories[0] == cache_dir_1);

	REQUIRE(config2_ref.cache_type == "in_mem");
	REQUIRE(config2_ref.cache_block_size == 8);
	REQUIRE(config2_ref.on_disk_cache_directories[0] == cache_dir_2);

	REQUIRE(config3_ref.cache_type == "noop");
	REQUIRE(config3_ref.cache_block_size == 10);
	REQUIRE(config3_ref.on_disk_cache_directories[0] == cache_dir_3);

	// Verify each instance can read the same file independently
	string content1(TEST_FILE_SIZE, '\0');
	{
		auto handle = cache_fs1->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		cache_fs1->Read(*handle, const_cast<void *>(static_cast<const void *>(content1.data())), TEST_FILE_SIZE, 0);
	}
	REQUIRE(content1 == TEST_FILE_CONTENT);

	string content2(TEST_FILE_SIZE, '\0');
	{
		auto handle = cache_fs2->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		cache_fs2->Read(*handle, const_cast<void *>(static_cast<const void *>(content2.data())), TEST_FILE_SIZE, 0);
	}
	REQUIRE(content2 == TEST_FILE_CONTENT);

	string content3(TEST_FILE_SIZE, '\0');
	{
		auto handle = cache_fs3->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		cache_fs3->Read(*handle, const_cast<void *>(static_cast<const void *>(content3.data())), TEST_FILE_SIZE, 0);
	}
	REQUIRE(content3 == TEST_FILE_CONTENT);
}

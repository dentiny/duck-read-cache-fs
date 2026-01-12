// Unit test for multiple DuckDB instances in a single process.
//
// This test validates that the extension properly supports multiple DuckDB instances in a single process, each with its
// own independent configuration and state.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem_config.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "scoped_directory.hpp"
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

void CreateSourceTestFile() {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();
}

} // namespace

// Test basic multiple instances with different cache types
TEST_CASE("Test multiple instances with different cache types", "[multi-instance test]") {
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
	TestCacheFileSystemHelper helper1(config1);
	auto *cache_fs1 = helper1.GetCacheFileSystem();
	auto &inst_state1 = helper1.GetInstanceStateOrThrow();
	auto &config1_ref = helper1.GetConfig();

	// Create second instance with in_mem cache
	TestCacheConfig config2;
	config2.cache_type = "in_mem";
	config2.cache_block_size = 8;
	config2.cache_directories = {cache_dir_2};
	TestCacheFileSystemHelper helper2(config2);
	auto *cache_fs2 = helper2.GetCacheFileSystem();
	auto &inst_state2 = helper2.GetInstanceStateOrThrow();
	auto &config2_ref = helper2.GetConfig();

	// Create third instance with noop cache
	TestCacheConfig config3;
	config3.cache_type = "noop";
	config3.cache_block_size = 10;
	config3.cache_directories = {cache_dir_3};
	TestCacheFileSystemHelper helper3(config3);
	auto *cache_fs3 = helper3.GetCacheFileSystem();
	auto &inst_state3 = helper3.GetInstanceStateOrThrow();
	auto &config3_ref = helper3.GetConfig();

	// Verify each instance has its own independent configuration
	REQUIRE(config1_ref.cache_type == "on_disk");
	REQUIRE(config1_ref.cache_block_size == 5);
	REQUIRE(config1_ref.on_disk_cache_directories[0] == cache_dir_1);

	REQUIRE(config2_ref.cache_type == "in_mem");
	REQUIRE(config2_ref.cache_block_size == 8);

	REQUIRE(config3_ref.cache_type == "noop");
	REQUIRE(config3_ref.cache_block_size == 10);

	// Verify instances are different objects
	REQUIRE(&inst_state1 != &inst_state2);
	REQUIRE(&inst_state2 != &inst_state3);
	REQUIRE(&inst_state1 != &inst_state3);

	// Perform operations on each instance
	{
		auto handle = cache_fs1->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = 10;
		string content(bytes_to_read, '\0');
		cache_fs1->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	{
		auto handle = cache_fs2->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 5;
		const uint64_t bytes_to_read = 8;
		string content(bytes_to_read, '\0');
		cache_fs2->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	{
		auto handle = cache_fs3->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 10;
		const uint64_t bytes_to_read = 10;
		string content(bytes_to_read, '\0');
		cache_fs3->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Verify cache files were created only for on_disk instance
	REQUIRE(GetFileCountUnder(cache_dir_1) > 0);
	REQUIRE(GetFileCountUnder(cache_dir_2) == 0);
	REQUIRE(GetFileCountUnder(cache_dir_3) == 0);
}

int main(int argc, char **argv) {
	CreateSourceTestFile();
	int result = Catch::Session().run(argc, argv);
	LocalFileSystem::CreateLocal()->RemoveFile(TEST_FILENAME);
	return result;
}

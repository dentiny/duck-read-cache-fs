// Unit test for exception propagation in cache filesystem.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "mock_filesystem.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

constexpr int64_t TEST_CHUNK_SIZE = 10;
const auto TEST_FILENAME = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_httpfs_cache_exception";

} // namespace

// Test that exceptions thrown during file read are correctly propagated for disk cache.
TEST_CASE("Test exception propagation on file read - disk cache", "[read exception test]") {
	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILE_SIZE);
	// Enable exception throwing on read operations
	mock_filesystem->SetThrowExceptionOnRead(true);

	// Create test helper which sets up the instance state
	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = TEST_CHUNK_SIZE;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	config.max_file_handle_cache_entry = 1;

	// We need a DuckDB instance for this test
	DuckDB db {};
	auto instance_state = GetInstanceStateShared(*db.instance.get());
	auto &inst_config = instance_state->config;
	inst_config.cache_type = config.cache_type;
	inst_config.cache_block_size = config.cache_block_size;
	inst_config.max_file_handle_cache_entry = config.max_file_handle_cache_entry;
	inst_config.on_disk_cache_directories = config.cache_directories;
	inst_config.enable_glob_cache = config.enable_glob_cache;
	SetInstanceState(*db.instance, instance_state);
	InitializeCacheReaderForTest(instance_state, inst_config);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));

	// Attempt to read from the cache filesystem - should throw IOException
	auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_CHUNK_SIZE;
	string content(bytes_to_read, '\0');

	// The read should throw an IOException because the mock filesystem is configured to throw
	REQUIRE_THROWS_AS(cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
	                                         bytes_to_read, start_offset),
	                  IOException);
}

// Test that exceptions are correctly propagated in parallel read operations for disk cache.
TEST_CASE("Test exception propagation in parallel reads - disk cache", "[read exception test]") {
	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILE_SIZE * 2); // Larger file to trigger multiple chunks
	// Enable exception throwing on read operations
	mock_filesystem->SetThrowExceptionOnRead(true);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = TEST_CHUNK_SIZE;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	config.max_file_handle_cache_entry = 1;

	DuckDB db {};
	auto instance_state = GetInstanceStateShared(*db.instance.get());
	auto &inst_config = instance_state->config;
	inst_config.cache_type = config.cache_type;
	inst_config.cache_block_size = config.cache_block_size;
	inst_config.max_file_handle_cache_entry = config.max_file_handle_cache_entry;
	inst_config.on_disk_cache_directories = config.cache_directories;
	inst_config.enable_glob_cache = config.enable_glob_cache;
	SetInstanceState(*db.instance, instance_state);
	InitializeCacheReaderForTest(instance_state, inst_config);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));

	// Attempt to read a range that spans multiple chunks - should throw IOException
	{
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = TEST_CHUNK_SIZE * 2; // Spans multiple chunks
		string content(bytes_to_read, '\0');

		// The read should throw an IOException because the mock filesystem is configured to throw
		// This tests exception propagation through the parallel read mechanism
		REQUIRE_THROWS_AS(cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
		                                         bytes_to_read, start_offset),
		                  IOException);
	}
}

// Test that exceptions thrown during file read are correctly propagated for in-memory cache.
TEST_CASE("Test exception propagation on file read - in-memory cache", "[read exception test]") {
	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILE_SIZE);
	// Enable exception throwing on read operations
	mock_filesystem->SetThrowExceptionOnRead(true);

	TestCacheConfig config;
	config.cache_type = "in_mem";
	config.cache_block_size = TEST_CHUNK_SIZE;
	config.max_file_handle_cache_entry = 1;

	DuckDB db {};
	auto instance_state = GetInstanceStateShared(*db.instance.get());
	auto &inst_config = instance_state->config;
	inst_config.cache_type = config.cache_type;
	inst_config.cache_block_size = config.cache_block_size;
	inst_config.max_file_handle_cache_entry = config.max_file_handle_cache_entry;
	inst_config.enable_glob_cache = config.enable_glob_cache;
	SetInstanceState(*db.instance, instance_state);
	InitializeCacheReaderForTest(instance_state, inst_config);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));

	// Attempt to read from the cache filesystem - should throw IOException
	auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_CHUNK_SIZE;
	string content(bytes_to_read, '\0');

	// The read should throw an IOException because the mock filesystem is configured to throw
	REQUIRE_THROWS_AS(cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
	                                         bytes_to_read, start_offset),
	                  IOException);
}

// Test that exceptions are correctly propagated in parallel read operations for in-memory cache.
TEST_CASE("Test exception propagation in parallel reads - in-memory cache", "[read exception test]") {
	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILE_SIZE * 2); // Larger file to trigger multiple chunks
	// Enable exception throwing on read operations
	mock_filesystem->SetThrowExceptionOnRead(true);

	TestCacheConfig config;
	config.cache_type = "in_mem";
	config.cache_block_size = TEST_CHUNK_SIZE;
	config.max_file_handle_cache_entry = 1;

	// We need a DuckDB instance for this test
	DuckDB db {};
	auto instance_state = GetInstanceStateShared(*db.instance.get());
	auto &inst_config = instance_state->config;
	inst_config.cache_type = config.cache_type;
	inst_config.cache_block_size = config.cache_block_size;
	inst_config.max_file_handle_cache_entry = config.max_file_handle_cache_entry;
	inst_config.enable_glob_cache = config.enable_glob_cache;
	SetInstanceState(*db.instance, instance_state);
	InitializeCacheReaderForTest(instance_state, inst_config);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));

	// Attempt to read a range that spans multiple chunks - should throw IOException
	auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_CHUNK_SIZE * 2; // Spans multiple chunks
	string content(bytes_to_read, '\0');

	// The read should throw an IOException because the mock filesystem is configured to throw
	// This tests exception propagation through the parallel read mechanism
	REQUIRE_THROWS_AS(cache_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
	                                         bytes_to_read, start_offset),
	                  IOException);
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}

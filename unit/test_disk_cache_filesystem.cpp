// Unit test for disk cache filesystem.
//
// IO operations are performed in chunks, testing senarios are listed as follows:
// (1) There're one or more chunks to read;
// (2) Chunks to read include start, middle and end parts of the file;
// (3) Bytes to read include partial or complete part of a chunk;
// (4) Chunks to read is not cached, partially cached, or completed cached.
// These senarios are orthogonal and should be tested in their combinations.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "mock_filesystem.hpp"
#include "scope_guard.hpp"
#include "test_utils.hpp"

#include <utime.h>

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
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_httpfs_cache";

void CreateSourceTestFile() {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();
}

// A filesystem wrapper that extends LocalFileSystem but allows setting version tags.
class VersionTagFileSystem : public LocalFileSystem {
public:
	void SetVersionTag(const string &tag) {
		version_tag = tag;
	}
	string GetVersionTag(FileHandle &handle) override {
		return version_tag;
	}

private:
	string version_tag;
};

} // namespace

// Test default directory works for uncached read.
TEST_CASE("Test on default cache directory", "[on-disk cache filesystem test]") {
	// Cleanup default cache directory before test.
	LocalFileSystem::CreateLocal()->RemoveDirectory(GetDefaultOnDiskCacheDirectory());

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_directories = {GetDefaultOnDiskCacheDirectory()};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// Uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	REQUIRE(GetFileCountUnder(GetDefaultOnDiskCacheDirectory()) > 0);
}

// One chunk is involved, requested bytes include only "first and last chunk".
TEST_CASE("Test on disk cache filesystem with requested chunk the first meanwhile last chunk",
          "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 26;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second cached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

// Two chunks are involved, which include both first and last chunks.
TEST_CASE("Test on disk cache filesystem with requested chunk the first and last chunk",
          "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 2;
		const uint64_t bytes_to_read = 5;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second cached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 3;
		const uint64_t bytes_to_read = 4;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

TEST_CASE("Test on disk cache filesystem with request for the last part of the file",
          "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 25;
		const uint64_t bytes_to_read = 1;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);

		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 1);

	// Second cached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 25;
		const uint64_t bytes_to_read = 1;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 1);
}

// Three blocks involved, which include first, last and middle chunk.
TEST_CASE("Test on disk cache filesystem with requested chunk the first, middle and last chunk",
          "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 2;
		const uint64_t bytes_to_read = 11;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second cached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 3;
		const uint64_t bytes_to_read = 10;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

// One block involved, it's the first meanwhile last block; requested content
// doesn't involve the end of the file.
TEST_CASE("Test on disk cache filesystem with requested chunk first and last one", "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 2;
		const uint64_t bytes_to_read = 2;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second cached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 3;
		const uint64_t bytes_to_read = 2;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

// Requested chunk involves the end of the file.
TEST_CASE("Test on disk cache filesystem with requested chunk at last of file", "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 23;
		const uint64_t bytes_to_read = 10;
		string content(3, '\0'); // effective offset range: [23, 25]
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Check cache files count.
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 2);

	// Second cached read, partial cached and another part uncached.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 15;
		const uint64_t bytes_to_read = 15;
		string content(11, '\0'); // effective offset range: [15, 25]
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Get all cache files and check file count.
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 3);
}

// Requested chunk involves the middle of the file.
TEST_CASE("Test on disk cache filesystem with requested chunk at middle of file", "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 16;
		const uint64_t bytes_to_read = 3;
		string content(bytes_to_read, '\0'); // effective offset range: [16, 18]
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Get all cache files and check file count.
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 1);

	// Second cached read, partial cached and another part uncached.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 8;
		const uint64_t bytes_to_read = 14;
		string content(bytes_to_read, '\0'); // effective offset range: [8, 21]
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Get all cache files and check file count.
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 4);
}

// All chunks cached locally, later access shouldn't create new cache file.
TEST_CASE("Test on disk cache filesystem no new cache file after a full cache", "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = TEST_FILE_SIZE;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Get all cache files.
	auto cache_files1 = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);

	// Second cached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 3;
		const uint64_t bytes_to_read = 10;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Get all cache files and check unchanged.
	auto cache_files2 = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(cache_files1 == cache_files2);
}

TEST_CASE("Test on reading non-existent file", "[on-disk cache filesystem test]") {
	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	REQUIRE_THROWS(disk_cache_fs->OpenFile("non-existent-file", FileOpenFlags::FILE_FLAGS_READ));
}

TEST_CASE("Test on zero-byte cache file", "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	const auto zero_byte_filename = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
	SCOPE_EXIT {
		LocalFileSystem::CreateLocal()->RemoveFile(zero_byte_filename);
	};
	{
		auto local_filesystem = LocalFileSystem::CreateLocal();
		auto file_handle = local_filesystem->OpenFile(
		    zero_byte_filename, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		file_handle->Sync();
		file_handle->Close();
	}

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// Check the empty file and verify no cache files are created.
	{
		auto handle = disk_cache_fs->OpenFile(zero_byte_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = 10;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		// Check all bytes remain null, since buffer should be unchanged for zero-byte file.
		REQUIRE(content.size() == bytes_to_read);
		for (size_t idx = 0; idx < content.size(); ++idx) {
			REQUIRE(content[idx] == '\0');
		}
	}

	// Verify no cache files were created for zero-byte file.
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 0);
}

TEST_CASE("Test on concurrent access", "[on-disk cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = 5;
	TestCacheFileSystemHelper helper(config);
	auto *on_disk_cache_fs = helper.GetCacheFileSystem();

	auto handle = on_disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ |
	                                                            FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_FILE_SIZE;

	// Spawn multiple threads to read through in-memory cache filesystem.
	constexpr idx_t THREAD_NUM = 200;
	vector<thread> reader_threads;
	reader_threads.reserve(THREAD_NUM);
	for (idx_t idx = 0; idx < THREAD_NUM; ++idx) {
		reader_threads.emplace_back([&]() {
			string content(bytes_to_read, '\0');
			on_disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())),
			                       bytes_to_read, start_offset);
			REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
		});
	}
	for (auto &cur_thd : reader_threads) {
		D_ASSERT(cur_thd.joinable());
		cur_thd.join();
	}
}

// Testing scenario: check timestamp-based eviction policy.
TEST_CASE("Test on insufficient disk space", "[on-disk cache filesystem test]") {
	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_FILE_SIZE;
	string content(bytes_to_read, '\0');

	// Create stale files, which should be deleted when insufficient disk space detected.
	LocalFileSystem::CreateLocal()->CreateDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	const string old_cache_file = StringUtil::Format("%s/file1", TEST_ON_DISK_CACHE_DIRECTORY);
	{
		auto file_handle = LocalFileSystem::CreateLocal()->OpenFile(
		    old_cache_file, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	}
	const time_t now = std::time(nullptr);
	const time_t two_day_ago = now - 48 * 60 * 60; // two days ago
	struct utimbuf updated_time;
	updated_time.actime = two_day_ago;
	updated_time.modtime = two_day_ago;
	REQUIRE(utime(old_cache_file.data(), &updated_time) == 0);

	// Phase 1: With insufficient disk space (very high min_disk_bytes_for_cache), eviction should happen
	{
		TestCacheConfig config;
		config.cache_type = "on_disk";
		config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
		config.enable_disk_reader_mem_cache = false;
		// Pretend there's no sufficient disk space by setting a very high min_disk_bytes_for_cache
		config.min_disk_bytes_for_cache = static_cast<idx_t>(-1); // UINT64_MAX
		TestCacheFileSystemHelper helper(config);
		auto *on_disk_cache_fs = helper.GetCacheFileSystem();

		auto handle = on_disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ |
		                                                            FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
		on_disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                       start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));

		// At this point, stale cache file has already been deleted.
		REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 0);
		REQUIRE(!LocalFileSystem::CreateLocal()->FileExists(old_cache_file));
	}

	// Phase 2: With normal config, caching should work
	{
		TestCacheConfig config;
		config.cache_type = "on_disk";
		config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
		config.enable_disk_reader_mem_cache = false;
		TestCacheFileSystemHelper helper(config);
		auto *on_disk_cache_fs = helper.GetCacheFileSystem();

		auto handle = on_disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ |
		                                                            FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
		on_disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                       start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
		REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 1);
	}
}

// Testing scenario: remove file should clear corresponding items in the cache.
TEST_CASE("Test on file removal", "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = test_block_size;
	config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
	TestCacheFileSystemHelper helper(config);
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = TEST_FILE_SIZE;
		string content(bytes_to_read, '\0');
		disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                    start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Now remove the file and on-disk cache file.
	disk_cache_fs->RemoveFile(TEST_FILENAME);
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 0);

	// Check source file existence.
	const bool exists = disk_cache_fs->FileExists(TEST_FILENAME);
	REQUIRE(!exists);

	// Re-create the immutable test file, otherwise other test cases will break.
	CreateSourceTestFile();
}

// Testing scenario: check lru-based eviction policy.
TEST_CASE("Test on lru eviction", "[on-disk cache filesystem test]") {
	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	LocalFileSystem::CreateLocal()->CreateDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_FILE_SIZE;
	string content(bytes_to_read, '\0');

	// Create existing file 1 and set modification timestamp to two days ago
	const string existing_file_1 = StringUtil::Format("%s/file1", TEST_ON_DISK_CACHE_DIRECTORY);
	{
		auto file_handle = LocalFileSystem::CreateLocal()->OpenFile(
		    existing_file_1, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	}
	{
		const time_t now = std::time(nullptr);
		const time_t two_day_ago = now - 48 * 60 * 60;
		struct utimbuf updated_time;
		updated_time.actime = two_day_ago;
		updated_time.modtime = two_day_ago;
		REQUIRE(utime(existing_file_1.data(), &updated_time) == 0);
	}

	// Create existing file 2 and set modification timestamp to one day ago
	const string existing_file_2 = StringUtil::Format("%s/file2", TEST_ON_DISK_CACHE_DIRECTORY);
	{
		auto file_handle = LocalFileSystem::CreateLocal()->OpenFile(
		    existing_file_2, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	}
	{
		const time_t now = std::time(nullptr);
		const time_t one_day_ago = now - 24 * 60 * 60;
		struct utimbuf updated_time;
		updated_time.actime = one_day_ago;
		updated_time.modtime = one_day_ago;
		REQUIRE(utime(existing_file_2.data(), &updated_time) == 0);
	}

	// With insufficient disk space config, older file should be evicted first (LRU policy)
	{
		TestCacheConfig config;
		config.cache_type = "on_disk";
		config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
		config.eviction_policy = "lru_sp";
		config.enable_disk_reader_mem_cache = false;
		config.min_disk_bytes_for_cache = static_cast<idx_t>(-1); // UINT64_MAX
		TestCacheFileSystemHelper helper(config);
		auto *on_disk_cache_fs = helper.GetCacheFileSystem();

		auto handle = on_disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ |
		                                                            FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
		on_disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                       start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));

		// At this point, the first existing file (oldest) has already been deleted, with the second one still kept.
		REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 1);
		REQUIRE(!LocalFileSystem::CreateLocal()->FileExists(existing_file_1));
		REQUIRE(LocalFileSystem::CreateLocal()->FileExists(existing_file_2));
	}

	// Second read with new helper should evict the remaining file
	{
		TestCacheConfig config;
		config.cache_type = "on_disk";
		config.cache_directories = {TEST_ON_DISK_CACHE_DIRECTORY};
		config.eviction_policy = "lru_sp";
		config.enable_disk_reader_mem_cache = false;
		config.min_disk_bytes_for_cache = static_cast<idx_t>(-1); // UINT64_MAX
		TestCacheFileSystemHelper helper(config);
		auto *on_disk_cache_fs = helper.GetCacheFileSystem();

		auto handle = on_disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ |
		                                                            FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
		on_disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                       start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));

		// At this point, both existing files have been deleted.
		REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 0);
		REQUIRE(!LocalFileSystem::CreateLocal()->FileExists(existing_file_1));
		REQUIRE(!LocalFileSystem::CreateLocal()->FileExists(existing_file_2));
	}
}

int main(int argc, char **argv) {
	CreateSourceTestFile();

	int result = Catch::Session().run(argc, argv);
	LocalFileSystem::CreateLocal()->RemoveFile(TEST_FILENAME);
	return result;
}

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
#include "scope_guard.hpp"

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
} // namespace

// Test default directory works for uncached read.
TEST_CASE("Test on default cache directory", "[on-disk cache filesystem test]") {
	// Cleanup default cache directory before test.
	LocalFileSystem::CreateLocal()->RemoveDirectory(*DEFAULT_ON_DISK_CACHE_DIRECTORY);
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

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

	REQUIRE(GetFileCountUnder(*DEFAULT_ON_DISK_CACHE_DIRECTORY) > 0);
}

// One chunk is involved, requested bytes include only "first and last chunk".
TEST_CASE("Test on disk cache filesystem with requested chunk the first meanwhile last chunk",
          "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 26;
	*g_on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
	g_cache_block_size = test_block_size;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

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
	*g_on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
	g_cache_block_size = test_block_size;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

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

// Three blocks involved, which include first, last and middle chunk.
TEST_CASE("Test on disk cache filesystem with requested chunk the first, middle and last chunk",
          "[on-disk cache filesystem test]") {
	constexpr uint64_t test_block_size = 5;
	*g_on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
	g_cache_block_size = test_block_size;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

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
	*g_on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
	g_cache_block_size = test_block_size;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

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
	*g_on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
	g_cache_block_size = test_block_size;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

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
	*g_on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
	g_cache_block_size = test_block_size;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

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
	*g_on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
	g_cache_block_size = test_block_size;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

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
	auto disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());
	REQUIRE_THROWS(disk_cache_fs->OpenFile("non-existent-file", FileOpenFlags::FILE_FLAGS_READ));
}

TEST_CASE("Test on concurrent access", "[on-disk cache filesystem test]") {
	g_cache_block_size = 5;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	auto on_disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

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

TEST_CASE("Test on insufficient disk space", "[on-disk cache filesystem test]") {
	SCOPE_EXIT {
		ResetGlobalConfig();
	};
	*g_on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
	LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);

	auto on_disk_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());
	auto handle = on_disk_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ |
	                                                            FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_FILE_SIZE;

	// Create stale files, which should be deleted when insufficient disk space detected.
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

	// Pretend there's no sufficient disk space, so cache file eviction is triggered.
	g_test_insufficient_disk_space = true;
	string content(bytes_to_read, '\0');
	on_disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
	                       start_offset);
	REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));

	// At this point, stale cache file has already been deleted.
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 0);
	REQUIRE(!LocalFileSystem::CreateLocal()->FileExists(old_cache_file));

	// Second access is uncached read.
	g_test_insufficient_disk_space = false;
	on_disk_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
	                       start_offset);
	REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 1);
}

int main(int argc, char **argv) {
	// Set global cache type for testing.
	*g_test_cache_type = *ON_DISK_CACHE_TYPE;

	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();

	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveFile(TEST_FILENAME);
	return result;
}

// Unit test for in-memory cache filesystem.

#include "catch/catch.hpp"

#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"
#include "mock_filesystem.hpp"
#include "scoped_directory.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

struct InMemoryCacheFilesystemFixture {
	ScopedDirectory scoped_dir;
	string test_filename;
	InMemoryCacheFilesystemFixture()
	    : scoped_dir(
	          StringUtil::Format("/tmp/duckdb_test_in_memory_cache_%s", UUID::ToString(UUID::GenerateRandomUUID()))) {
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

// Helper struct to create a CacheFileSystem with a custom internal filesystem
struct CustomFsHelper {
	DuckDB db;
	shared_ptr<CacheHttpfsInstanceState> instance_state;
	unique_ptr<CacheFileSystem> cache_fs;

	CustomFsHelper(unique_ptr<FileSystem> internal_fs, const TestCacheConfig &config) {
		instance_state = make_shared_ptr<CacheHttpfsInstanceState>();

		// Configure the instance
		auto &inst_config = instance_state->config;
		inst_config.cache_type = config.cache_type;
		inst_config.cache_block_size = config.cache_block_size;
		inst_config.enable_cache_validation = config.enable_cache_validation;
		inst_config.enable_disk_reader_mem_cache = config.enable_disk_reader_mem_cache;

		// Register state with instance
		SetInstanceState(*db.instance.get(), instance_state);
		InitializeCacheReaderForTest(instance_state, inst_config);

		// Create cache filesystem wrapping the provided filesystem
		cache_fs = make_uniq<CacheFileSystem>(std::move(internal_fs), instance_state);
	}

	CacheFileSystem *GetCacheFileSystem() {
		return cache_fs.get();
	}
};

// Helper struct to create a CacheFileSystem with a MockFileSystem (keeps pointer to mock)
struct MockFsHelper {
	DuckDB db;
	shared_ptr<CacheHttpfsInstanceState> instance_state;
	unique_ptr<CacheFileSystem> cache_fs;
	MockFileSystem *mock_fs_ptr = nullptr; // Non-owning pointer for accessing mock filesystem

	MockFsHelper(unique_ptr<MockFileSystem> mock_fs, const TestCacheConfig &config) {
		instance_state = make_shared_ptr<CacheHttpfsInstanceState>();

		// Configure the instance
		auto &inst_config = instance_state->config;
		inst_config.cache_type = config.cache_type;
		inst_config.cache_block_size = config.cache_block_size;
		inst_config.enable_cache_validation = config.enable_cache_validation;
		inst_config.enable_disk_reader_mem_cache = config.enable_disk_reader_mem_cache;

		// Register state with instance
		SetInstanceState(*db.instance.get(), instance_state);
		InitializeCacheReaderForTest(instance_state, inst_config);

		// Create cache filesystem wrapping the provided filesystem
		mock_fs_ptr = mock_fs.get();
		cache_fs = make_uniq<CacheFileSystem>(std::move(mock_fs), instance_state);
	}

	CacheFileSystem *GetCacheFileSystem() {
		return cache_fs.get();
	}
};
} // namespace

TEST_CASE_METHOD(InMemoryCacheFilesystemFixture, "Test on in-memory cache filesystem",
                 "[in-memory cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "in_mem";
	config.cache_block_size = TEST_FILE_SIZE;
	TestCacheFileSystemHelper helper(std::move(config));
	auto *in_mem_cache_fs = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = in_mem_cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second cached read.
	{
		auto handle = in_mem_cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

TEST_CASE_METHOD(InMemoryCacheFilesystemFixture, "Test on concurrent access - in-memory cache",
                 "[in-memory cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "in_mem";
	config.cache_block_size = 5;
	TestCacheFileSystemHelper helper(std::move(config));
	auto *in_mem_cache_fs = helper.GetCacheFileSystem();

	auto handle = in_mem_cache_fs->OpenFile(test_filename,
	                                        FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_FILE_SIZE;

	// Spawn multiple threads to read through in-memory cache filesystem.
	constexpr idx_t THREAD_NUM = 200;
	vector<thread> reader_threads;
	reader_threads.reserve(THREAD_NUM);
	for (idx_t idx = 0; idx < THREAD_NUM; ++idx) {
		reader_threads.emplace_back([&]() {
			string content(bytes_to_read, '\0');
			in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
			                      start_offset);
			REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
		});
	}
	for (auto &cur_thd : reader_threads) {
		D_ASSERT(cur_thd.joinable());
		cur_thd.join();
	}
}

TEST_CASE_METHOD(InMemoryCacheFilesystemFixture, "Test cache validation disabled",
                 "[in-memory cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "in_mem";
	config.cache_block_size = TEST_FILE_SIZE;
	config.enable_cache_validation = false;

	CustomFsHelper helper(LocalFileSystem::CreateLocal(), config);
	auto *in_mem_cache_fs = helper.GetCacheFileSystem();

	// First read, should cache.
	{
		auto handle = in_mem_cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = TEST_FILE_SIZE;
		string content(bytes_to_read, '\0');
		in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second read, should use cache even if file metadata changes (validation disabled).
	{
		auto handle = in_mem_cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = TEST_FILE_SIZE;
		string content(bytes_to_read, '\0');
		in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

TEST_CASE_METHOD(InMemoryCacheFilesystemFixture, "Test cache validation with version tag",
                 "[in-memory cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "in_mem";
	config.cache_block_size = TEST_FILE_SIZE;
	config.enable_cache_validation = true;

	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILE_SIZE);
	mock_filesystem->SetVersionTag("v1");
	mock_filesystem->SetLastModificationTime(timestamp_t {1000000});

	MockFsHelper helper(std::move(mock_filesystem), config);
	auto *mock_filesystem_ptr = helper.mock_fs_ptr;
	auto *in_mem_cache_fs = helper.GetCacheFileSystem();

	// First read, should cache with version tag "v1".
	{
		auto handle = in_mem_cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = TEST_FILE_SIZE;
		string content(bytes_to_read, '\0');
		in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == std::string(TEST_FILE_SIZE, 'a'));
		REQUIRE(mock_filesystem_ptr->GetVersionTagInvocation() == 1);
	}

	// Second read with same version tag, should use cache.
	{
		mock_filesystem_ptr->SetVersionTag("v1");
		auto handle = in_mem_cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = TEST_FILE_SIZE;
		string content(bytes_to_read, '\0');
		in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == std::string(TEST_FILE_SIZE, 'a'));
		// Should validate but use cache.
		REQUIRE(mock_filesystem_ptr->GetVersionTagInvocation() == 2);
	}

	// Third read with different version tag, should invalidate cache and re-read.
	{
		mock_filesystem_ptr->SetVersionTag("v2");
		mock_filesystem_ptr->ClearReadOperations();
		auto handle = in_mem_cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 0;
		const uint64_t bytes_to_read = TEST_FILE_SIZE;
		string content(bytes_to_read, '\0');
		in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == std::string(TEST_FILE_SIZE, 'a'));
		// Should validate, invalidate cache, and re-read from mock filesystem.
		REQUIRE(mock_filesystem_ptr->GetVersionTagInvocation() == 3);
		auto read_operations = mock_filesystem_ptr->GetSortedReadOperations();
		REQUIRE(read_operations.size() == 1);
		REQUIRE(read_operations[0].start_offset == 0);
		REQUIRE(read_operations[0].bytes_to_read == TEST_FILE_SIZE);
	}
}

#include "cache_httpfs_instance_state.hpp"
#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "mock_filesystem.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {
const std::string TEST_FILENAME = "filename";
const std::string TEST_GLOB_NAME = "*"; // Need to contain glob characters.
constexpr int64_t TEST_FILESIZE = 26;
constexpr int64_t TEST_CHUNK_SIZE = 5;

void TestReadWithMockFileSystem(const TestCacheConfig &base_config) {
	uint64_t close_invocation = 0;
	uint64_t dtor_invocation = 0;
	auto close_callback = [&close_invocation]() {
		++close_invocation;
	};
	auto dtor_callback = [&dtor_invocation]() {
		++dtor_invocation;
	};

	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	auto *mock_filesystem_ptr = mock_filesystem.get();

	// Create test helper which sets up the instance state
	TestCacheConfig config = base_config;
	config.cache_block_size = TEST_CHUNK_SIZE;
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

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));

	// Uncached read.
	{
		// Make sure it's mock file handle.
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto &cache_file_handle = handle->Cast<CacheFileSystemHandle>();
		[[maybe_unused]] auto &mock_file_handle = cache_file_handle.internal_file_handle->Cast<MockFileHandle>();

		std::string buffer(TEST_FILESIZE, '\0');
		cache_filesystem->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, /*location=*/0);
		REQUIRE(buffer == std::string(TEST_FILESIZE, 'a'));

		auto read_operations = mock_filesystem_ptr->GetSortedReadOperations();
		REQUIRE(read_operations.size() == 6);
		for (idx_t idx = 0; idx < 6; ++idx) {
			REQUIRE(read_operations[idx].start_offset == idx * TEST_CHUNK_SIZE);
			REQUIRE(read_operations[idx].bytes_to_read ==
			        MinValue<int64_t>(TEST_CHUNK_SIZE, TEST_FILESIZE - idx * TEST_CHUNK_SIZE));
		}

		// Glob operation.
		REQUIRE(cache_filesystem->Glob(TEST_GLOB_NAME).empty());
	}

	// Cache read.
	{
		// Make sure it's mock file handle.
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto &cache_file_handle = handle->Cast<CacheFileSystemHandle>();
		[[maybe_unused]] auto &mock_file_handle = cache_file_handle.internal_file_handle->Cast<MockFileHandle>();

		// Create a new handle, which cannot leverage the cached one.
		auto another_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		[[maybe_unused]] auto &another_mock_handle =
		    another_handle->Cast<CacheFileSystemHandle>().internal_file_handle->Cast<MockFileHandle>();

		std::string buffer(TEST_FILESIZE, '\0');
		cache_filesystem->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, /*location=*/0);
		REQUIRE(buffer == std::string(TEST_FILESIZE, 'a'));

		mock_filesystem_ptr->ClearReadOperations();
		auto read_operations = mock_filesystem_ptr->GetSortedReadOperations();
		REQUIRE(read_operations.empty());

		// Glob operation.
		REQUIRE(cache_filesystem->Glob(TEST_GLOB_NAME).empty());

		// [handle] and [another_handle] go out of scope and place back to file handle, but due to insufficient capacity
		// only one of them will be cached and another one closed and destructed.
	}

	// One of the file handles resides in cache, another one gets closed and destructed.
	REQUIRE(close_invocation == 1);
	REQUIRE(dtor_invocation == 1);
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 1);

	// Destructing the cache filesystem cleans file handle cache, which in turns close and destruct all cached file
	// handles.
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 2);

	cache_filesystem = nullptr;
	REQUIRE(close_invocation == 2);
	REQUIRE(dtor_invocation == 2);
}

} // namespace

// Testing scenario: if glob operation returns file size, it should get cached.
TEST_CASE("Test file metadata cache for glob invocation", "[mock filesystem test]") {
	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));

	// An implementation detail: file path to glob needs to contain glob characters, otherwise cache filesystem doesn't
	// try to cache anything.
	const string FILE_PATTERN_WITH_GLOB = "*";

	// Set file info for glob operation.
	OpenFileInfo open_info {TEST_FILENAME};
	// Mimic s3 filesystem implementation, to make sure file size does correctly cached.
	// Ref:
	// https://github.com/duckdb/duckdb-httpfs/blob/cb5b2825eff68fc91f47e917ba88bf2ed84c2dd3/extension/httpfs/s3fs.cpp#L1171
	string size_str = "10";
	string last_modification_time_str = "2024-11-09T11:38:08.000Z"; // Unix timestamp 1731152288.
	auto extended_file_info = make_shared_ptr<ExtendedOpenFileInfo>();
	extended_file_info->options.emplace("file_size",
	                                    Value(size_str).DefaultCastAs(LogicalType {LogicalTypeId::UBIGINT}));
	extended_file_info->options.emplace(
	    "last_modified", Value(last_modification_time_str).DefaultCastAs(LogicalType {LogicalTypeId::TIMESTAMP}));
	open_info.extended_info = std::move(extended_file_info);

	vector<OpenFileInfo> glob_returns;
	glob_returns.emplace_back(std::move(open_info));
	mock_filesystem->SetGlobResults(std::move(glob_returns));

	// Set an incorrect file size for mock file system, to make sure it's not called.
	mock_filesystem->SetFileSize(20);
	mock_filesystem->SetLastModificationTime(timestamp_t {-1});

	// Perform glob and get file size operation.
	auto *mock_filesystem_ptr = mock_filesystem.get();

	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));
	cache_filesystem->Glob(FILE_PATTERN_WITH_GLOB);
	auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	const int64_t file_size = cache_filesystem->GetFileSize(*file_handle);
	const timestamp_t last_modification_time = cache_filesystem->GetLastModifiedTime(*file_handle);

	// Check invocation results.
	REQUIRE(file_size == 10);
	REQUIRE(last_modification_time == timestamp_t {1731152288000000});
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 1);
	REQUIRE(mock_filesystem_ptr->GetSizeInvocation() == 0);
	REQUIRE(mock_filesystem_ptr->GetLastModTimeInvocation() == 0);
}

// Testing scenario: when any file attribute is accessed, all of them should be cached.
TEST_CASE("Test file attribute for glob invocation", "[mock filesystem test]") {
	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(20);
	mock_filesystem->SetLastModificationTime(timestamp_t {1731152288});

	// Get file size and last modification timestamp.
	auto *mock_filesystem_ptr = mock_filesystem.get();
	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));
	auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	{
		[[maybe_unused]] const int64_t file_size = cache_filesystem->GetFileSize(*file_handle);
		[[maybe_unused]] const timestamp_t last_modification_time = cache_filesystem->GetLastModifiedTime(*file_handle);
	}

	// Fetch file attributes again.
	const int64_t file_size = cache_filesystem->GetFileSize(*file_handle);
	const timestamp_t last_modification_time = cache_filesystem->GetLastModifiedTime(*file_handle);

	// Check invocation results.
	REQUIRE(file_size == 20);
	REQUIRE(last_modification_time == timestamp_t {1731152288});
	REQUIRE(mock_filesystem_ptr->GetSizeInvocation() == 1);
	REQUIRE(mock_filesystem_ptr->GetLastModTimeInvocation() == 1);
}

TEST_CASE("Test disk cache reader with mock filesystem", "[mock filesystem test]") {
	for (const auto &cur_cache_dir : {*DEFAULT_ON_DISK_CACHE_DIRECTORY}) {
		LocalFileSystem::CreateLocal()->RemoveDirectory(cur_cache_dir);
	}

	TestCacheConfig config;
	config.cache_type = "on_disk";
	config.cache_block_size = TEST_CHUNK_SIZE;
	config.max_file_handle_cache_entry = 1;
	TestReadWithMockFileSystem(config);
}

TEST_CASE("Test in-memory cache reader with mock filesystem", "[mock filesystem test]") {
	for (const auto &cur_cache_dir : {*DEFAULT_ON_DISK_CACHE_DIRECTORY}) {
		LocalFileSystem::CreateLocal()->RemoveDirectory(cur_cache_dir);
	}

	TestCacheConfig config;
	config.cache_type = "in_mem";
	config.cache_block_size = TEST_CHUNK_SIZE;
	config.max_file_handle_cache_entry = 1;
	TestReadWithMockFileSystem(config);
}

TEST_CASE("Test clear cache", "[mock filesystem test]") {
	uint64_t close_invocation = 0;
	uint64_t dtor_invocation = 0;
	auto close_callback = [&close_invocation]() {
		++close_invocation;
	};
	auto dtor_callback = [&dtor_invocation]() {
		++dtor_invocation;
	};

	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	auto *mock_filesystem_ptr = mock_filesystem.get();

	// Create cache filesystem without database instance for this simpler test
	TestCacheConfig config;
	config.cache_type = "noop";
	config.max_file_handle_cache_entry = 1;

	DuckDB db {};
	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	instance_state->config.max_file_handle_cache_entry = config.max_file_handle_cache_entry;
	SetInstanceState(*db.instance, instance_state);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));

	auto perform_io_operation = [&]() {
		auto handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		REQUIRE(cache_filesystem->Glob(TEST_GLOB_NAME).empty());
	};

	// Uncached IO operations.
	perform_io_operation();
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 1);
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 1);

	// Clear cache and perform IO operations.
	cache_filesystem->ClearCache();
	perform_io_operation();
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 2);
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 2);

	// Clear cache by filepath and perform IO operation.
	cache_filesystem->ClearCache(TEST_FILENAME);
	cache_filesystem->ClearCache(TEST_GLOB_NAME);
	perform_io_operation();
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 3);
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 3);

	// Retry one cached IO operation.
	perform_io_operation();
	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 3);
	REQUIRE(mock_filesystem_ptr->GetFileOpenInvocation() == 3);
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

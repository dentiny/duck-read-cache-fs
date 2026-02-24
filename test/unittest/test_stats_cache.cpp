#include "catch/catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string.hpp"
#include "mock_filesystem.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

class MockFileSystemWithStats : public MockFileSystem {
public:
	MockFileSystemWithStats(std::function<void()> close_callback_p, std::function<void()> dtor_callback_p)
	    : MockFileSystem(std::move(close_callback_p), std::move(dtor_callback_p)) {
	}

	FileMetadata Stats(FileHandle &handle) override {
		const concurrency::lock_guard<concurrency::mutex> lck(stats_mtx);
		++stats_invocation;
		FileMetadata metadata;
		// Use stored values directly (set via SetFileSize/SetLastModificationTime from parent)
		// We need to call parent methods to get the values, but this will increment counters
		// So we'll adjust test expectations accordingly
		metadata.file_size = MockFileSystem::GetFileSize(handle);
		metadata.last_modification_time = MockFileSystem::GetLastModifiedTime(handle);
		metadata.file_type = FileType::FILE_TYPE_REGULAR;
		return metadata;
	}

	uint64_t GetStatsInvocation() const {
		const concurrency::lock_guard<concurrency::mutex> lck(stats_mtx);
		return stats_invocation;
	}

private:
	mutable concurrency::mutex stats_mtx;
	uint64_t stats_invocation DUCKDB_GUARDED_BY(stats_mtx) = 0;
};

class MockFileSystemWithoutStats : public MockFileSystem {
public:
	MockFileSystemWithoutStats(std::function<void()> close_callback_p, std::function<void()> dtor_callback_p)
	    : MockFileSystem(std::move(close_callback_p), std::move(dtor_callback_p)) {
	}

	FileMetadata Stats(FileHandle &handle) override {
		throw NotImplementedException("Stats is not implemented");
	}
};

const std::string TEST_FILENAME = "test_file";
constexpr int64_t TEST_FILESIZE = 1024;
constexpr timestamp_t TEST_LAST_MOD_TIME = timestamp_t {1731152288000000};

} // namespace

TEST_CASE("Test Stats cache with Stats() supported", "[stats cache test]") {
	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystemWithStats>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	mock_filesystem->SetLastModificationTime(TEST_LAST_MOD_TIME);
	auto *mock_filesystem_ptr = mock_filesystem.get();

	// Create instance state with metadata cache enabled
	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	instance_state->config.enable_metadata_cache = true;
	instance_state->config.max_metadata_cache_entry = 10;
	instance_state->config.metadata_cache_entry_timeout_millisec = 0; // No timeout
	InitializeCacheReaderForTest(instance_state, instance_state->config);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));
	auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);

	// First call: should be a cache miss, `Stats` should be called
	{
		const int64_t file_size = cache_filesystem->GetFileSize(*file_handle);
		REQUIRE(file_size == TEST_FILESIZE);
		REQUIRE(mock_filesystem_ptr->GetStatsInvocation() == 1);
		// Stats() calls GetFileSize() and GetLastModifiedTime() internally, so counters will be >= 1
		REQUIRE(mock_filesystem_ptr->GetSizeInvocation() >= 1);
		REQUIRE(mock_filesystem_ptr->GetLastModTimeInvocation() >= 1);
	}

	// Second call: should be a cache hit, `Stats` should not be called again
	{
		const uint64_t prev_size_invocations = mock_filesystem_ptr->GetSizeInvocation();
		const uint64_t prev_mod_time_invocations = mock_filesystem_ptr->GetLastModTimeInvocation();
		const int64_t file_size = cache_filesystem->GetFileSize(*file_handle);
		REQUIRE(file_size == TEST_FILESIZE);
		REQUIRE(mock_filesystem_ptr->GetStatsInvocation() == 1); // Still 1, not called again
		// Counters should not have increased (using cache)
		REQUIRE(mock_filesystem_ptr->GetSizeInvocation() == prev_size_invocations);
		REQUIRE(mock_filesystem_ptr->GetLastModTimeInvocation() == prev_mod_time_invocations);
	}

	// Test GetLastModifiedTime also uses cached metadata
	{
		const uint64_t prev_mod_time_invocations = mock_filesystem_ptr->GetLastModTimeInvocation();
		const timestamp_t last_mod_time = cache_filesystem->GetLastModifiedTime(*file_handle);
		REQUIRE(last_mod_time == TEST_LAST_MOD_TIME);
		REQUIRE(mock_filesystem_ptr->GetStatsInvocation() == 1); // Still 1, using cache
		// Counter should not have increased (using cache)
		REQUIRE(mock_filesystem_ptr->GetLastModTimeInvocation() == prev_mod_time_invocations);
	}

	// Test Stats() method directly - should use cache
	{
		const uint64_t prev_stats_invocations = mock_filesystem_ptr->GetStatsInvocation();
		const FileMetadata metadata = cache_filesystem->Stats(*file_handle);
		REQUIRE(metadata.file_size == TEST_FILESIZE);
		REQUIRE(metadata.last_modification_time == TEST_LAST_MOD_TIME);
		// Stats() should use the cache, so counter should not have increased
		REQUIRE(mock_filesystem_ptr->GetStatsInvocation() == prev_stats_invocations);
	}
}

TEST_CASE("Test Stats cache fallback when Stats() not supported", "[stats cache test]") {
	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystemWithoutStats>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	mock_filesystem->SetLastModificationTime(TEST_LAST_MOD_TIME);
	auto *mock_filesystem_ptr = mock_filesystem.get();

	// Create instance state with metadata cache enabled
	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	instance_state->config.enable_metadata_cache = true;
	instance_state->config.max_metadata_cache_entry = 10;
	instance_state->config.metadata_cache_entry_timeout_millisec = 0; // No timeout
	InitializeCacheReaderForTest(instance_state, instance_state->config);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));
	auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);

	// First call: should be a cache miss, fallback to GetFileSize() and GetLastModifiedTime()
	{
		const int64_t file_size = cache_filesystem->GetFileSize(*file_handle);
		REQUIRE(file_size == TEST_FILESIZE);
		// Should have called GetFileSize() and GetLastModifiedTime() once each (for fallback)
		REQUIRE(mock_filesystem_ptr->GetSizeInvocation() == 1);
		REQUIRE(mock_filesystem_ptr->GetLastModTimeInvocation() == 1);
	}

	// Second call: should be a cache hit, no additional calls
	{
		const int64_t file_size = cache_filesystem->GetFileSize(*file_handle);
		REQUIRE(file_size == TEST_FILESIZE);
		// Should not have called again (using cache)
		REQUIRE(mock_filesystem_ptr->GetSizeInvocation() == 1);
		REQUIRE(mock_filesystem_ptr->GetLastModTimeInvocation() == 1);
	}

	// Test GetLastModifiedTime also uses cached metadata
	{
		const timestamp_t last_mod_time = cache_filesystem->GetLastModifiedTime(*file_handle);
		REQUIRE(last_mod_time == TEST_LAST_MOD_TIME);
		// Should not have called again (using cache)
		REQUIRE(mock_filesystem_ptr->GetSizeInvocation() == 1);
		REQUIRE(mock_filesystem_ptr->GetLastModTimeInvocation() == 1);
	}
}

TEST_CASE("Test Stats cache with metadata cache disabled", "[stats cache test]") {
	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystemWithStats>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	mock_filesystem->SetLastModificationTime(TEST_LAST_MOD_TIME);
	auto *mock_filesystem_ptr = mock_filesystem.get();

	// Create instance state with metadata cache disabled
	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	instance_state->config.enable_metadata_cache = false;
	InitializeCacheReaderForTest(instance_state, instance_state->config);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));
	auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);

	// Without cache, each call should go directly to internal filesystem
	{
		const int64_t file_size = cache_filesystem->GetFileSize(*file_handle);
		REQUIRE(file_size == TEST_FILESIZE);
		// Should call GetFileSize() directly (not Stats() because cache is disabled)
		REQUIRE(mock_filesystem_ptr->GetSizeInvocation() == 1);
		REQUIRE(mock_filesystem_ptr->GetStatsInvocation() == 0);
	}

	// Second call: should call again since no cache
	{
		const int64_t file_size = cache_filesystem->GetFileSize(*file_handle);
		REQUIRE(file_size == TEST_FILESIZE);
		REQUIRE(mock_filesystem_ptr->GetSizeInvocation() == 2);
		REQUIRE(mock_filesystem_ptr->GetStatsInvocation() == 0);
	}
}

TEST_CASE("Test Stats() method directly on CacheFileSystem", "[stats cache test]") {
	auto close_callback = []() {
	};
	auto dtor_callback = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystemWithStats>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	mock_filesystem->SetLastModificationTime(TEST_LAST_MOD_TIME);
	auto *mock_filesystem_ptr = mock_filesystem.get();

	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	InitializeCacheReaderForTest(instance_state, instance_state->config);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));
	auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);

	// Stats() should delegate directly to internal filesystem
	const FileMetadata metadata = cache_filesystem->Stats(*file_handle);
	REQUIRE(metadata.file_size == TEST_FILESIZE);
	REQUIRE(metadata.last_modification_time == TEST_LAST_MOD_TIME);
	REQUIRE(mock_filesystem_ptr->GetStatsInvocation() == 1);
}

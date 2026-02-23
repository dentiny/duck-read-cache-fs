
#include "catch/catch.hpp"

#include "base_profile_collector.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"
#include "in_memory_cache_reader.hpp"
#include "noop_cache_reader.hpp"
#include "noop_profile_collector.hpp"
#include "temp_profile_collector.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

struct FilesystemConfigFixture {
	string test_filename;
	FilesystemConfigFixture() {
		test_filename = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
		auto local_filesystem = LocalFileSystem::CreateLocal();
		auto file_handle = local_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		// File created (empty); original main() just created empty file
		file_handle->Close();
	}
	~FilesystemConfigFixture() {
		LocalFileSystem::CreateLocal()->RemoveFile(test_filename);
	}
};

} // namespace

TEST_CASE("Filesystem config test", "[filesystem config]") {
	REQUIRE(GetThreadCountForSubrequests(10, 0) == 10);
	REQUIRE(GetThreadCountForSubrequests(10, 5) == 5);
}

TEST_CASE_METHOD(FilesystemConfigFixture, "Filesystem cache config test", "[filesystem config]") {
	// Test that different cache types can be configured via per-instance configuration
	{
		// Test noop cache reader
		TestCacheConfig config;
		config.cache_type = "noop";
		TestCacheFileSystemHelper helper(std::move(config));
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		auto *cache_reader = helper.GetInstanceStateOrThrow().cache_reader_manager.GetCacheReader();
		REQUIRE(cache_reader != nullptr);
		[[maybe_unused]] auto &noop_handle = cache_reader->Cast<NoopCacheReader>();
	}

	{
		// Test in-memory cache reader
		TestCacheConfig config;
		config.cache_type = "in_mem";
		TestCacheFileSystemHelper helper(std::move(config));
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		auto *cache_reader = helper.GetInstanceStateOrThrow().cache_reader_manager.GetCacheReader();
		REQUIRE(cache_reader != nullptr);
		[[maybe_unused]] auto &in_mem_handle = cache_reader->Cast<InMemoryCacheReader>();
	}

	{
		// Test disk cache reader
		TestCacheConfig config;
		config.cache_type = "on_disk";
		TestCacheFileSystemHelper helper(std::move(config));
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		auto *cache_reader = helper.GetInstanceStateOrThrow().cache_reader_manager.GetCacheReader();
		REQUIRE(cache_reader != nullptr);
		[[maybe_unused]] auto &disk_handle = cache_reader->Cast<DiskCacheReader>();
	}
}

TEST_CASE_METHOD(FilesystemConfigFixture, "Filesystem profile config test", "[filesystem config]") {
	// Check noop profiler.
	{
		TestCacheConfig config;
		config.profile_type = "noop";
		TestCacheFileSystemHelper helper(std::move(config));
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		auto &profiler = cache_fs->GetProfileCollector();
		[[maybe_unused]] auto &noop_profiler = profiler.Cast<NoopProfileCollector>();
	}

	// Check temp profiler.
	{
		TestCacheConfig config;
		config.profile_type = "temp";
		TestCacheFileSystemHelper helper(std::move(config));
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		auto &profiler = cache_fs->GetProfileCollector();
		[[maybe_unused]] auto &temp_profiler = profiler.Cast<TempProfileCollector>();
	}
}

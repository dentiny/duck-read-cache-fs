#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

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
#include "temp_profile_collector.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {
const auto TEST_FILENAME = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
} // namespace

TEST_CASE("Filesystem config test", "[filesystem config]") {
	REQUIRE(GetThreadCountForSubrequests(10) == 10);
	REQUIRE(GetThreadCountForSubrequests(10, 5) == 5);
}

TEST_CASE("Filesystem cache config test", "[filesystem config]") {
	// Test that different cache types can be configured via per-instance configuration
	{
		// Test noop cache reader
		TestCacheConfig config;
		config.cache_type = "noop";
		TestCacheFileSystemHelper helper(config);
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto *cache_reader = helper.GetInstanceState()->cache_reader_manager.GetCacheReader();
		REQUIRE(cache_reader != nullptr);
		[[maybe_unused]] auto &noop_handle = cache_reader->Cast<NoopCacheReader>();
	}

	{
		// Test in-memory cache reader
		TestCacheConfig config;
		config.cache_type = "in_mem";
		TestCacheFileSystemHelper helper(config);
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto *cache_reader = helper.GetInstanceState()->cache_reader_manager.GetCacheReader();
		REQUIRE(cache_reader != nullptr);
		[[maybe_unused]] auto &in_mem_handle = cache_reader->Cast<InMemoryCacheReader>();
	}

	{
		// Test disk cache reader
		TestCacheConfig config;
		config.cache_type = "on_disk";
		TestCacheFileSystemHelper helper(config);
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto *cache_reader = helper.GetInstanceState()->cache_reader_manager.GetCacheReader();
		REQUIRE(cache_reader != nullptr);
		[[maybe_unused]] auto &disk_handle = cache_reader->Cast<DiskCacheReader>();
	}
}

TEST_CASE("Filesystem profile config test", "[filesystem config]") {
	// Check noop profiler.
	{
		TestCacheConfig config;
		config.profile_type = "noop";
		TestCacheFileSystemHelper helper(config);
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto *profiler = helper.GetProfileCollector();
		[[maybe_unused]] auto &noop_profiler = profiler->Cast<NoopProfileCollector>();
	}

	// Check temp profiler.
	{
		TestCacheConfig config;
		config.profile_type = "temp";
		TestCacheFileSystemHelper helper(config);
		auto *cache_fs = helper.GetCacheFileSystem();

		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		auto *profiler = helper.GetProfileCollector();
		[[maybe_unused]] auto &temp_profiler = profiler->Cast<TempProfileCollector>();
	}
}

int main(int argc, char **argv) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveFile(TEST_FILENAME);
	return result;
}

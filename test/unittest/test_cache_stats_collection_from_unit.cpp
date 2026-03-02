// This test file validates cache hit, cache miss and cache in-use count.

#include "catch/catch.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "scoped_directory.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

const string TEST_CONTENT = "helloworld";

struct CacheStatsFixture {
	ScopedDirectory scoped_dir;
	string test_filepath;
	CacheStatsFixture()
	    : scoped_dir(
	          StringUtil::Format("/tmp/duckdb_test_cache_stats_%s", UUID::ToString(UUID::GenerateRandomUUID()))) {
		test_filepath = StringUtil::Format("%s/testfile", scoped_dir.GetPath());
		auto local_filesystem = LocalFileSystem::CreateLocal();
		auto file_handle = local_filesystem->OpenFile(test_filepath, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<char *>(TEST_CONTENT.data()), TEST_CONTENT.length(),
		                        /*location=*/0);
		file_handle->Sync();
	}
};

// Get cache access info for file handle.
CacheAccessInfo GetFileHandleCacheInfo(BaseProfileCollector &profiler) {
	auto cache_access_infos = profiler.GetCacheAccessInfo();
	for (auto cur_access_info : cache_access_infos) {
		if (cur_access_info.cache_type == "file handle") {
			return cur_access_info;
		}
	}
	D_ASSERT(false); // Unreachable.
	return CacheAccessInfo {};
}

} // namespace

TEST_CASE_METHOD(CacheStatsFixture, "Test cache stats collection disabled", "[profile collector]") {
	TestCacheConfig config;
	config.cache_type = "noop";
	config.profile_type = "noop";
	config.enable_file_handle_cache = true;
	TestCacheFileSystemHelper helper(std::move(config));
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// First access, there're no cache entries inside of cache filesystem.
	[[maybe_unused]] auto file_handle_1 = cache_filesystem->OpenFile(test_filepath, FileOpenFlags::FILE_FLAGS_READ);
	auto *profiler = helper.GetProfileCollector();
	REQUIRE(profiler != nullptr);
	auto file_handle_cache_info = GetFileHandleCacheInfo(*profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 0);

	// Second access, still cache miss, but indicate we should have bigger cache size.
	[[maybe_unused]] auto file_handle_2 = cache_filesystem->OpenFile(test_filepath, FileOpenFlags::FILE_FLAGS_READ);
	file_handle_cache_info = GetFileHandleCacheInfo(*profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 0);
}

TEST_CASE_METHOD(CacheStatsFixture, "Test cache stats collection", "[profile collector]") {
	TestCacheConfig config;
	config.cache_type = "noop";
	config.profile_type = "temp";
	config.enable_file_handle_cache = true;
	TestCacheFileSystemHelper helper(std::move(config));
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// First access, there're no cache entries inside of cache filesystem.
	[[maybe_unused]] auto file_handle_1 = cache_filesystem->OpenFile(test_filepath, FileOpenFlags::FILE_FLAGS_READ);
	auto *profiler = helper.GetProfileCollector();
	REQUIRE(profiler != nullptr);
	auto file_handle_cache_info = GetFileHandleCacheInfo(*profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_count == 1);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 0);

	// Second access, still cache miss, but indicate we should have bigger cache size.
	[[maybe_unused]] auto file_handle_2 = cache_filesystem->OpenFile(test_filepath, FileOpenFlags::FILE_FLAGS_READ);
	file_handle_cache_info = GetFileHandleCacheInfo(*profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_count == 2);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 1);

	// Third access, place internal file handle back to file handle cache.
	file_handle_1.reset();
	file_handle_2.reset();
	[[maybe_unused]] auto file_handle_3 = cache_filesystem->OpenFile(test_filepath, FileOpenFlags::FILE_FLAGS_READ);
	file_handle_cache_info = GetFileHandleCacheInfo(*profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 1);
	REQUIRE(file_handle_cache_info.cache_miss_count == 2);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 1);
}

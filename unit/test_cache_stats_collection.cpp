// This test file validate cache hit, cache miss and cache in-use count.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <string>

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "scope_guard.hpp"

using namespace duckdb; // NOLINT

namespace {

const std::string TEST_CONTENT = "helloworld";
const std::string TEST_FILEPATH = "/tmp/testfile";
void CreateTestFile() {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILEPATH, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<char *>(TEST_CONTENT.data()), TEST_CONTENT.length(),
	                        /*location=*/0);
	file_handle->Sync();
}
void DeleteTestFile() {
	LocalFileSystem::CreateLocal()->RemoveFile(TEST_FILEPATH);
}

// Get cache access info for file handle.
CacheAccessInfo GetFileHandleCacheInfo(BaseProfileCollector *profiler) {
	auto cache_access_infos = profiler->GetCacheAccessInfo();
	for (auto cur_access_info : cache_access_infos) {
		if (cur_access_info.cache_type == "file handle") {
			return cur_access_info;
		}
	}
	D_ASSERT(false); // Unreachable.
}

} // namespace

TEST_CASE("Test cache stats collection disabled", "[profile collector]") {
	*g_profile_type = *NOOP_PROFILE_TYPE;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	// First access, there're no cache entries inside of cache filesystem.
	auto cache_filesystem = make_uniq<CacheFileSystem>(make_uniq<LocalFileSystem>());
	[[maybe_unused]] auto file_handle_1 = cache_filesystem->OpenFile(TEST_FILEPATH, FileOpenFlags::FILE_FLAGS_READ);
	auto *profiler = cache_filesystem->GetProfileCollector();
	auto file_handle_cache_info = GetFileHandleCacheInfo(profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 0);

	// Second access, still cache miss, but indicate we should have bigger cache size.
	[[maybe_unused]] auto file_handle_2 = cache_filesystem->OpenFile(TEST_FILEPATH, FileOpenFlags::FILE_FLAGS_READ);
	profiler = cache_filesystem->GetProfileCollector();
	file_handle_cache_info = GetFileHandleCacheInfo(profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 0);
}

TEST_CASE("Test cache stats collection", "[profile collector]") {
	*g_profile_type = *TEMP_PROFILE_TYPE;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	// First access, there're no cache entries inside of cache filesystem.
	auto cache_filesystem = make_uniq<CacheFileSystem>(make_uniq<LocalFileSystem>());
	[[maybe_unused]] auto file_handle_1 = cache_filesystem->OpenFile(TEST_FILEPATH, FileOpenFlags::FILE_FLAGS_READ);
	auto *profiler = cache_filesystem->GetProfileCollector();
	auto file_handle_cache_info = GetFileHandleCacheInfo(profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_count == 1);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 0);

	// Second access, still cache miss, but indicate we should have bigger cache size.
	[[maybe_unused]] auto file_handle_2 = cache_filesystem->OpenFile(TEST_FILEPATH, FileOpenFlags::FILE_FLAGS_READ);
	profiler = cache_filesystem->GetProfileCollector();
	file_handle_cache_info = GetFileHandleCacheInfo(profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 0);
	REQUIRE(file_handle_cache_info.cache_miss_count == 2);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 1);

	// Third access, place internal file handle back to file handle cache.
	file_handle_1.reset();
	file_handle_2.reset();
	[[maybe_unused]] auto file_handle_3 = cache_filesystem->OpenFile(TEST_FILEPATH, FileOpenFlags::FILE_FLAGS_READ);
	profiler = cache_filesystem->GetProfileCollector();
	file_handle_cache_info = GetFileHandleCacheInfo(profiler);
	REQUIRE(file_handle_cache_info.cache_hit_count == 1);
	REQUIRE(file_handle_cache_info.cache_miss_count == 2);
	REQUIRE(file_handle_cache_info.cache_miss_by_in_use == 1);
}

int main(int argc, char **argv) {
	CreateTestFile();
	int result = Catch::Session().run(argc, argv);
	DeleteTestFile();
	return result;
}

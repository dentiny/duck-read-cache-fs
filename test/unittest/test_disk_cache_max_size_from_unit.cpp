// Unit test for max on-disk cache size enforcement.
//
// Testing scenarios:
// (1) Footprint tracker scans finalized cache files lazily, skips in-flight temporary files, and gets incrementally
// updated afterwards;
// (2) With LRU eviction policy, cache file writes which would exceed the cap trigger eviction, and the total cache
// file size stays bounded;
// (3) With creation timestamp eviction policy, the cache stops growing at the cap when no file is stale, and stale
// file eviction reclaims space so caching resumes.

#include "catch/catch.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_footprint_tracker.hpp"
#include "disk_cache_util.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "scoped_directory.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"

#include <ctime>

using namespace duckdb; // NOLINT

namespace {

constexpr uint64_t TEST_BLOCK_SIZE = 4;
// Cap the cache at two full blocks.
constexpr uint64_t TEST_MAX_ON_DISK_CACHE_SIZE = 2 * TEST_BLOCK_SIZE;

struct MaxCacheSizeFixture {
	ScopedDirectory scoped_cache_dir;
	ScopedDirectory scoped_source_dir;
	string test_filename;

	MaxCacheSizeFixture()
	    : scoped_cache_dir(
	          StringUtil::Format("/tmp/duckdb_test_max_cache_size_%s", UUID::ToString(UUID::GenerateRandomUUID()))),
	      scoped_source_dir(StringUtil::Format("/tmp/duckdb_test_max_cache_size_src_%s",
	                                           UUID::ToString(UUID::GenerateRandomUUID()))) {
		test_filename = StringUtil::Format("%s/source_file", scoped_source_dir.GetPath());
		auto local_filesystem = LocalFileSystem::CreateLocal();
		auto file_handle = local_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
		                        TEST_FILE_SIZE, /*location=*/0);
		file_handle->Sync();
		file_handle->Close();
	}

	TestCacheConfig GetTestConfig(const string &eviction_policy) const {
		TestCacheConfig config;
		config.cache_type = "on_disk";
		config.cache_block_size = TEST_BLOCK_SIZE;
		config.cache_directories = {scoped_cache_dir.GetPath()};
		config.eviction_policy = eviction_policy;
		config.max_on_disk_cache_size = TEST_MAX_ON_DISK_CACHE_SIZE;
		// Disable the read-through/write-through memory cache, so every read exercises the on-disk cache path.
		config.enable_disk_reader_mem_cache = false;
		return config;
	}

	// Read the block starting at [start_offset] through [cache_fs] and validate its content.
	void ReadAndCheckBlock(FileSystem &cache_fs, idx_t start_offset) const {
		auto handle = cache_fs.OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const idx_t bytes_to_read = MinValue<idx_t>(TEST_BLOCK_SIZE, TEST_FILE_SIZE - start_offset);
		string content(bytes_to_read, '\0');
		cache_fs.Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		              start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
};

// Get the total size for all finalized cache files under [folder].
idx_t GetTotalCacheFileSizeUnder(const string &folder) {
	idx_t total_bytes = 0;
	auto local_filesystem = LocalFileSystem::CreateLocal();
	local_filesystem->ListFiles(folder, [&](const string &fname, bool /*unused*/) {
		if (DiskCacheUtil::IsTempCacheFile(fname)) {
			return;
		}
		auto file_handle =
		    local_filesystem->OpenFile(StringUtil::Format("%s/%s", folder, fname), FileOpenFlags::FILE_FLAGS_READ);
		total_bytes += static_cast<idx_t>(local_filesystem->GetFileSize(*file_handle));
	});
	return total_bytes;
}

void CreateFileWithSize(const string &filepath, idx_t file_size) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(filepath, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                            FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	const string content(file_size, 'a');
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(content.data())), file_size,
	                        /*location=*/0);
	file_handle->Sync();
	file_handle->Close();
}

} // namespace

TEST_CASE("Footprint tracker scans lazily and updates incrementally", "[max on-disk cache size test]") {
	ScopedDirectory scoped_dir(
	    StringUtil::Format("/tmp/duckdb_test_footprint_tracker_%s", UUID::ToString(UUID::GenerateRandomUUID())));
	CreateFileWithSize(StringUtil::Format("%s/cache-file-1", scoped_dir.GetPath()), 10);
	CreateFileWithSize(StringUtil::Format("%s/cache-file-2", scoped_dir.GetPath()), 20);
	// In-flight temporary cache files are excluded from the scan.
	CreateFileWithSize(StringUtil::Format("%s/cache-file-3.%s.httpfs_local_cache", scoped_dir.GetPath(),
	                                      UUID::ToString(UUID::GenerateRandomUUID())),
	                   100);
	const vector<string> cache_directories {scoped_dir.GetPath()};

	DiskCacheFootprintTracker footprint_tracker;

	// Updates before the first load are no-ops.
	REQUIRE(!footprint_tracker.IsLoaded());
	footprint_tracker.Add(1000);
	footprint_tracker.Subtract(1000);
	REQUIRE(!footprint_tracker.IsLoaded());

	// First access scans the cache directories.
	REQUIRE(footprint_tracker.GetOrLoad(cache_directories) == 30);
	REQUIRE(footprint_tracker.IsLoaded());

	// Later accesses return the incrementally-updated value without re-scan.
	footprint_tracker.Add(5);
	REQUIRE(footprint_tracker.GetOrLoad(cache_directories) == 35);
	auto local_filesystem = LocalFileSystem::CreateLocal();
	footprint_tracker.SubtractFileSize(*local_filesystem, StringUtil::Format("%s/cache-file-2", scoped_dir.GetPath()));
	REQUIRE(footprint_tracker.GetOrLoad(cache_directories) == 15);
	// Subtraction is floored at zero, and non-existent files subtract nothing.
	footprint_tracker.SubtractFileSize(*local_filesystem, StringUtil::Format("%s/non-existent", scoped_dir.GetPath()));
	footprint_tracker.Subtract(1000);
	REQUIRE(footprint_tracker.GetOrLoad(cache_directories) == 0);

	// Invalidation drops the tracked value, so the next access re-scans.
	footprint_tracker.Invalidate();
	REQUIRE(!footprint_tracker.IsLoaded());
	REQUIRE(footprint_tracker.GetOrLoad(cache_directories) == 30);
}

TEST_CASE_METHOD(MaxCacheSizeFixture, "Max cache size with LRU eviction policy", "[max on-disk cache size test]") {
	TestCacheFileSystemHelper helper(GetTestConfig(/*eviction_policy=*/"lru_sp"));
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// Read the whole file block by block; the total cache file size stays bounded by the cap the whole time.
	for (idx_t start_offset = 0; start_offset < TEST_FILE_SIZE; start_offset += TEST_BLOCK_SIZE) {
		ReadAndCheckBlock(*disk_cache_fs, start_offset);
		REQUIRE(GetTotalCacheFileSizeUnder(scoped_cache_dir.GetPath()) <= TEST_MAX_ON_DISK_CACHE_SIZE);
	}

	// The file spans more blocks than the cap allows, so eviction must have kicked in.
	REQUIRE(GetFileCountUnder(scoped_cache_dir.GetPath()) >= 1);
	REQUIRE(GetFileCountUnder(scoped_cache_dir.GetPath()) <= 2);

	// Re-reads over the partially evicted cache still return correct content and respect the cap.
	for (idx_t start_offset = 0; start_offset < TEST_FILE_SIZE; start_offset += TEST_BLOCK_SIZE) {
		ReadAndCheckBlock(*disk_cache_fs, start_offset);
		REQUIRE(GetTotalCacheFileSizeUnder(scoped_cache_dir.GetPath()) <= TEST_MAX_ON_DISK_CACHE_SIZE);
	}
}

TEST_CASE_METHOD(MaxCacheSizeFixture, "Max cache size with creation timestamp eviction policy",
                 "[max on-disk cache size test]") {
	TestCacheFileSystemHelper helper(GetTestConfig(/*eviction_policy=*/"creation_timestamp"));
	auto *disk_cache_fs = helper.GetCacheFileSystem();

	// Read the whole file block by block; no cache file is stale, so the cache simply stops growing at the cap.
	for (idx_t start_offset = 0; start_offset < TEST_FILE_SIZE; start_offset += TEST_BLOCK_SIZE) {
		ReadAndCheckBlock(*disk_cache_fs, start_offset);
	}
	REQUIRE(GetTotalCacheFileSizeUnder(scoped_cache_dir.GetPath()) == TEST_MAX_ON_DISK_CACHE_SIZE);
	REQUIRE(GetFileCountUnder(scoped_cache_dir.GetPath()) == 2);

	// Make the cached files stale, so the next over-cap cache file write evicts them all.
	vector<string> cached_files;
	LocalFileSystem::CreateLocal()->ListFiles(scoped_cache_dir.GetPath(), [&](const string &fname, bool /*unused*/) {
		cached_files.emplace_back(StringUtil::Format("%s/%s", scoped_cache_dir.GetPath(), fname));
	});
	const time_t stale_mtime = std::time(nullptr) - static_cast<time_t>(CACHE_FILE_STALENESS_SECOND) - 100;
	for (const auto &cached_file : cached_files) {
		SetFileMtime(cached_file, stale_mtime);
	}

	// The over-cap write evicts all stale files and skips its own store.
	ReadAndCheckBlock(*disk_cache_fs, /*start_offset=*/2 * TEST_BLOCK_SIZE);
	REQUIRE(GetFileCountUnder(scoped_cache_dir.GetPath()) == 0);

	// With the space reclaimed, caching resumes on the next read.
	ReadAndCheckBlock(*disk_cache_fs, /*start_offset=*/3 * TEST_BLOCK_SIZE);
	REQUIRE(GetFileCountUnder(scoped_cache_dir.GetPath()) == 1);
	REQUIRE(GetTotalCacheFileSizeUnder(scoped_cache_dir.GetPath()) == TEST_BLOCK_SIZE);
}

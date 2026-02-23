#include "catch/catch.hpp"

#include "cache_filesystem_config.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "filesystem_utils.hpp"
#include "mock_filesystem.hpp"
#include "scoped_directory.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

constexpr int64_t TEST_FILESIZE = 26;
constexpr int64_t TEST_CHUNK_SIZE = 26;
const auto TEST_DIRECTORY = "/tmp/duckdb_test_oversized_cache_filepath";

// Build a filename that exceeds the platform's max filename length.
struct OversizedFile {
	string filepath;
	string filename;
};
OversizedFile MakeOversizedFilepath() {
	const auto limits = GetMaxFileNameLength();
	const string long_component(limits.max_filename_len + 100, 'x');
	return OversizedFile {
	    .filepath = StringUtil::Format("s3://bucket/%s.parquet", long_component),
	    .filename = StringUtil::Format("%s.parquet", long_component),
	};
}

} // namespace

// The first read misses all caches and falls through to the mock filesystem.
// The data is then stored in both on-disk and in-memory caches under the resolved (sha256-shortened) filepath.
// A second read must be served entirely from cache, so the mock filesystem must see zero additional Read calls.
TEST_CASE("Oversized filename caching roundtrip", "[disk_cache_util]") {
	ScopedDirectory scoped_dir(TEST_DIRECTORY);
	LocalFileSystem local_fs {};

	const auto oversized_file = MakeOversizedFilepath();
	auto mock_filesystem = make_uniq<MockFileSystem>([]() {}, []() {});
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	auto *mock_fs_ptr = mock_filesystem.get();

	TestCacheConfig config;
	config.internal_filesystem = std::move(mock_filesystem);
	config.cache_type = *ON_DISK_CACHE_TYPE;
	config.cache_block_size = TEST_CHUNK_SIZE;
	config.cache_directories = {TEST_DIRECTORY};
	config.enable_disk_reader_mem_cache = true;
	TestCacheFileSystemHelper helper(std::move(config));
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// First read: cache miss, should fall through to mock filesystem.
	{
		auto handle = cache_filesystem->OpenFile(oversized_file.filepath, FileOpenFlags::FILE_FLAGS_READ);
		string buffer(TEST_FILESIZE, '\0');
		cache_filesystem->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, /*location=*/0);
		REQUIRE(buffer == string(TEST_FILESIZE, 'a'));

		auto read_ops = mock_fs_ptr->GetSortedReadOperations();
		REQUIRE(read_ops.size() == 1);
		REQUIRE(read_ops[0].start_offset == 0);
		REQUIRE(read_ops[0].bytes_to_read == TEST_FILESIZE);
	}

	// Verify the on-disk cache file was created with a shortened (sha256) name, not the oversized original.
	{
		const auto limits = GetMaxFileNameLength();
		vector<string> cached_files;
		local_fs.ListFiles(TEST_DIRECTORY, [&](const string &fname, bool) { cached_files.emplace_back(fname); });
		REQUIRE(cached_files.size() == 1);
		REQUIRE(cached_files[0].length() <= limits.max_filename_len);
	}

	// Clear recorded reads so we can assert no new ones on the second access.
	mock_fs_ptr->ClearReadOperations();

	// Second read: should be a cache hit (in-memory or on-disk), no mock reads.
	{
		auto handle = cache_filesystem->OpenFile(oversized_file.filepath, FileOpenFlags::FILE_FLAGS_READ);
		string buffer(TEST_FILESIZE, '\0');
		cache_filesystem->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, /*location=*/0);
		REQUIRE(buffer == string(TEST_FILESIZE, 'a'));

		auto read_ops = mock_fs_ptr->GetSortedReadOperations();
		REQUIRE(read_ops.empty());
	}

	// Check cache entries info.
	auto &cache_reader = helper.GetCacheReader();
	auto cache_entries_info = cache_reader.GetCacheEntriesInfo();
	REQUIRE(cache_entries_info.size() == 2);

	// In-memory disk cache cache is returned first.
	REQUIRE(cache_entries_info[0].cache_type == "in-mem-disk-cache");
	REQUIRE(cache_entries_info[0].remote_filename == oversized_file.filename);
	REQUIRE(cache_entries_info[0].start_offset == 0);
	REQUIRE(cache_entries_info[0].end_offset == TEST_FILESIZE);

	// Then comes on-disk cache.
	REQUIRE(cache_entries_info[1].cache_type == "on-disk");
	REQUIRE(cache_entries_info[1].remote_filename == oversized_file.filename);
	REQUIRE(cache_entries_info[1].start_offset == 0);
	REQUIRE(cache_entries_info[1].end_offset == TEST_FILESIZE);
}

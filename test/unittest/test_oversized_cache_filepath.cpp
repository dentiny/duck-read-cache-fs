#include "catch/catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "filesystem_utils.hpp"
#include "mock_filesystem.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

constexpr int64_t TEST_FILESIZE = 26;
constexpr int64_t TEST_CHUNK_SIZE = 26;

// Build a filename that exceeds the platform's max filename length.
string MakeOversizedFilename() {
	const auto limits = GetMaxFileNameLength();
	const string long_component(limits.max_filename_len + 100, 'x');
	return StringUtil::Format("s3://bucket/%s.parquet", long_component);
}

} // namespace

// The first read misses all caches and falls through to the mock filesystem.
// The data is then stored in both on-disk and in-memory caches under the resolved (sha256-shortened) filepath.
// A second read must be served entirely from cache, so the mock filesystem must see zero additional Read calls.
TEST_CASE("Oversized filename caching roundtrip", "[disk_cache_util]") {
	const auto &cache_dir = GetDefaultOnDiskCacheDirectory();
	auto local_fs = LocalFileSystem::CreateLocal();
	local_fs->RemoveDirectory(cache_dir);
	local_fs->CreateDirectory(cache_dir);

	const string test_filename = MakeOversizedFilename();

	auto close_cb = []() {
	};
	auto dtor_cb = []() {
	};
	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_cb), std::move(dtor_cb));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	auto *mock_fs_ptr = mock_filesystem.get();

	DuckDB db {};
	auto instance_state = GetInstanceStateShared(*db.instance.get());
	auto &inst_config = instance_state->config;
	inst_config.cache_type = "on_disk";
	inst_config.cache_block_size = TEST_CHUNK_SIZE;
	inst_config.max_file_handle_cache_entry = 1;
	inst_config.on_disk_cache_directories = {cache_dir};
	inst_config.enable_disk_reader_mem_cache = true;
	SetInstanceState(*db.instance, instance_state);
	InitializeCacheReaderForTest(instance_state, inst_config);

	auto cache_filesystem = make_uniq<CacheFileSystem>(std::move(mock_filesystem), std::move(instance_state));

	// First read: cache miss, should fall through to mock filesystem.
	{
		auto handle = cache_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
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
		local_fs->ListFiles(cache_dir, [&](const string &fname, bool) { cached_files.emplace_back(fname); });
		REQUIRE(cached_files.size() == 1);
		REQUIRE(cached_files[0].length() <= limits.max_filename_len);
	}

	// Clear recorded reads so we can assert no new ones on the second access.
	mock_fs_ptr->ClearReadOperations();

	// Second read: should be a cache hit (in-memory or on-disk), no mock reads.
	{
		auto handle = cache_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		string buffer(TEST_FILESIZE, '\0');
		cache_filesystem->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, /*location=*/0);
		REQUIRE(buffer == string(TEST_FILESIZE, 'a'));

		auto read_ops = mock_fs_ptr->GetSortedReadOperations();
		REQUIRE(read_ops.empty());
	}

	local_fs->RemoveDirectory(cache_dir);
}

#include "catch/catch.hpp"

#include "disk_cache_util.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "hash_utils.hpp"
#include "scoped_directory.hpp"

#include <ctime>
#if !defined(_WIN32)
#include <utime.h>
#endif

using namespace duckdb;

namespace {

// Reassemble chunked file attributes.
string ReassembleFileAttrs(const unordered_map<string, string> &file_attrs) {
	vector<string> keys;
	keys.reserve(file_attrs.size());
	for (const auto &kv : file_attrs) {
		keys.emplace_back(kv.first);
	}
	std::sort(keys.begin(), keys.end());

	string reassembled;
	for (const auto &key : keys) {
		reassembled += file_attrs.at(key);
	}
	return reassembled;
}

} // namespace

// Testing scenario: the cache destination for multiple cache directories is deterministic for same input.
TEST_CASE("Deterministic cache destination for same input", "[disk_cache_util]") {
	vector<string> cache_dirs = {"/cache1", "/cache2", "/cache3"};
	const string remote = "https://example.com/data/file.parquet";

	auto r1 = DiskCacheUtil::GetLocalCacheFile(cache_dirs, remote, /*start_offset=*/0, /*bytes_to_read=*/4096);
	auto r2 = DiskCacheUtil::GetLocalCacheFile(cache_dirs, remote, /*start_offset=*/0, /*bytes_to_read=*/4096);

	REQUIRE(r1.cache_directory_idx == r2.cache_directory_idx);
	REQUIRE(r1.cache_filepath == r2.cache_filepath);
}

TEST_CASE("Get remote file information from local cache filename", "[disk_cache_util]") {
	// fname format: <64 hex>-<remote-fname>-<start>-<block-size>
	string hash64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
	string fname = hash64 + "-file.parquet-0-4096";

	auto info = DiskCacheUtil::GetRemoteFileInfo(fname);
	REQUIRE(info.remote_filename == "file.parquet");
	REQUIRE(info.start_offset == 0);
	REQUIRE(info.end_offset == 4096);
}

TEST_CASE("DiskCacheUtil::GetLocalCacheFilePrefix - query and fragment stripped", "[disk_cache_util]") {
	const string url_plain = "https://example.com/file.parquet";
	const string url_with_query = "https://example.com/file.parquet?version=1";

	REQUIRE(DiskCacheUtil::GetLocalCacheFilePrefix(url_plain) ==
	        DiskCacheUtil::GetLocalCacheFilePrefix(url_with_query));
}

TEST_CASE("ResolveLocalCacheDestination - normal filepath, no fallback for oversized filepath and filename",
          "[disk_cache_util]") {
	const string cache_dir = "/tmp/cache";
	const string local_cache_file = "/tmp/cache/abc123-file.parquet-0-4096";

	auto result = DiskCacheUtil::ResolveLocalCacheDestination(cache_dir, local_cache_file);
	REQUIRE(result.dest_local_filepath == local_cache_file);
	REQUIRE(StringUtil::StartsWith(result.temp_local_filepath, local_cache_file));
	REQUIRE(StringUtil::EndsWith(result.temp_local_filepath, ".httpfs_local_cache"));
	REQUIRE(result.file_attrs.empty());
}

TEST_CASE("ResolveLocalCacheDestination - oversized filename triggers fallback", "[disk_cache_util]") {
	const string cache_dir = "/tmp/cache";
	const auto limits = GetMaxFileNameLength();

	const string long_name(limits.max_filename_len + 100, 'x');
	const string local_cache_file = StringUtil::Format("%s/%s", cache_dir, long_name);

	auto result = DiskCacheUtil::ResolveLocalCacheDestination(cache_dir, local_cache_file);
	const auto expected_sha = GetSha256(local_cache_file);
	REQUIRE(result.dest_local_filepath == StringUtil::Format("%s/%s", cache_dir, expected_sha));
	REQUIRE(StringUtil::StartsWith(result.temp_local_filepath, result.dest_local_filepath));
	REQUIRE(StringUtil::EndsWith(result.temp_local_filepath, ".httpfs_local_cache"));
	REQUIRE_FALSE(result.file_attrs.empty());
	REQUIRE(ReassembleFileAttrs(result.file_attrs) == local_cache_file);
}

TEST_CASE("ResolveLocalCacheDestination - oversized filepath triggers fallback", "[disk_cache_util]") {
	const auto limits = GetMaxFileNameLength();

	const string deep_dir(limits.max_filepath_len, 'd');
	const string cache_dir = StringUtil::Format("/%s", deep_dir);
	const string local_cache_file = StringUtil::Format("%s/file.parquet", cache_dir);

	auto result = DiskCacheUtil::ResolveLocalCacheDestination(cache_dir, local_cache_file);
	const auto expected_sha = GetSha256(local_cache_file);
	REQUIRE(result.dest_local_filepath == StringUtil::Format("%s/%s", cache_dir, expected_sha));
	REQUIRE_FALSE(result.file_attrs.empty());
	REQUIRE(ReassembleFileAttrs(result.file_attrs) == local_cache_file);
}

#if !defined(_WIN32)
TEST_CASE("CleanupDeadTempFiles deletes only stale temp files", "[disk_cache_util]") {
	const string test_dir =
	    StringUtil::Format("/tmp/test_disk_cache_util_%s", UUID::ToString(UUID::GenerateRandomUUID()));
	ScopedDirectory dir(test_dir);
	auto fs = LocalFileSystem::CreateLocal();

	// Temp files use suffix .httpfs_local_cache
	const string stale_path = StringUtil::Format("%s/stale.httpfs_local_cache", test_dir);
	const string fresh_path = StringUtil::Format("%s/fresh.httpfs_local_cache", test_dir);

	{
		auto h1 = fs->OpenFile(stale_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		fs->Write(*h1, const_cast<char *>("a"), /*nr_bytes=*/1, /*location=*/0);
		fs->FileSync(*h1);
	}
	{
		auto h2 = fs->OpenFile(fresh_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		fs->Write(*h2, const_cast<char *>("b"), /*nr_bytes=*/1, /*location=*/0);
		fs->FileSync(*h2);
	}

	// Set stale_path mtime to 1 year ago to make it stale.
	const time_t now = std::time(nullptr);
	const time_t eleven_min_ago = now - 365 * 24 * 60 * 60;
	struct utimbuf old_time;
	old_time.actime = eleven_min_ago;
	old_time.modtime = eleven_min_ago;
	REQUIRE(utime(stale_path.c_str(), &old_time) == 0);

	const idx_t deleted = DiskCacheUtil::CleanupDeadTempFiles({test_dir});
	REQUIRE(deleted == 1);
	REQUIRE(!fs->FileExists(stale_path));
	REQUIRE(fs->FileExists(fresh_path));
}
#endif

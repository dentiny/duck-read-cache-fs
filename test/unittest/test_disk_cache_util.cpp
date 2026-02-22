#include "catch/catch.hpp"

#include "disk_cache_util.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;

// Testing scenario: the cache destination for multiple cache directories is deterministic for same input.
TEST_CASE("Deterministic cache destination for same input", "[disk_cache_util]") {
	vector<string> cache_dirs = {"/cache1", "/cache2", "/cache3"};
	const string remote = "https://example.com/data/file.parquet";

	auto r1 = DiskCacheUtil::GetLocalCacheFile(cache_dirs, remote, 0, 4096);
	auto r2 = DiskCacheUtil::GetLocalCacheFile(cache_dirs, remote, 0, 4096);

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

#include "catch/catch.hpp"

#include "cache_httpfs_instance_state.hpp"
#include "disk_cache_util.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "page_aligned_data_chunk.hpp"
#include "scoped_directory.hpp"

#include <algorithm>
#include <cstring>

using namespace duckdb; // NOLINT

namespace {

// Generate a repeating-pattern byte vector of [len] bytes seeded from [seed].
vector<char> PatternBytes(idx_t len, char seed) {
	vector<char> out(len);
	for (idx_t i = 0; i < len; i++) {
		out[i] = static_cast<char>(seed + static_cast<char>(i % 23));
	}
	return out;
}

// Build an InstanceConfig that uses [cache_dir] as the sole cache directory.
InstanceConfig MakeConfig(const string &cache_dir, idx_t block_size) {
	InstanceConfig config;
	config.on_disk_cache_directories = {cache_dir};
	config.cache_block_size = block_size;
	config.enable_disk_reader_mem_cache = false;
	return config;
}

// Write a cache file for [remote_path] at byte range [start_offset, start_offset+content.size()).
void WriteCacheFile(const string &cache_dir, const string &remote_path, idx_t start_offset, const vector<char> &content,
                    const string &version_tag, const InstanceConfig &config) {
	auto dest = DiskCacheUtil::GetLocalCacheFile({cache_dir}, remote_path, start_offset, content.size());
	auto local_dest = DiskCacheUtil::ResolveLocalCacheDestination(cache_dir, dest.cache_filepath, remote_path);
	auto chunk = AllocatePageAlignedChunk(content.size());
	std::memcpy(chunk.data(), content.data(), content.size());
	chunk.length = content.size();
	DiskCacheUtil::StoreLocalCacheFile(cache_dir, local_dest, chunk, version_tag, config,
	                                   []() -> string { return {}; });
}

// Return true if a cache file exists for [remote_path] at [start_offset] with [size] bytes.
bool CacheFileExists(const string &cache_dir, const string &remote_path, idx_t start_offset, idx_t size) {
	auto dest = DiskCacheUtil::GetLocalCacheFile({cache_dir}, remote_path, start_offset, size);
	auto fs = LocalFileSystem::CreateLocal();
	return fs->FileExists(dest.cache_filepath);
}

// Read and return the content of a cache file for [remote_path] at [start_offset] with [size] bytes.
vector<char> ReadCacheFile(const string &cache_dir, const string &remote_path, idx_t start_offset, idx_t size) {
	auto dest = DiskCacheUtil::GetLocalCacheFile({cache_dir}, remote_path, start_offset, size);
	auto result =
	    DiskCacheUtil::ReadLocalCacheFile(dest.cache_filepath, size, /*version_tag=*/"", DiskCacheUtil::ReadOption {});
	REQUIRE(result.cache_hit);
	vector<char> out(result.content.data(), result.content.data() + size);
	return out;
}

} // namespace

// Case 1: A block whose start offset is not aligned to the new block grid is discarded.
//
// Setup: old_block_size=4096, new_block_size=8192.
//   Write only the second half of a potential 8192-byte block: [4096, 8192).
// Expected: AlignUpToBlock(4096, 8192) = 8192 which is NOT < run_end = 8192, so nothing is emitted.
//   The old file is deleted and no new file exists.
TEST_CASE("RemapOnDiskCacheEntries: block not aligned to new grid is discarded", "[disk cache remap]") {
	ScopedDirectory scoped_dir(
	    StringUtil::Format("/tmp/test_disk_cache_remap_discard_%s", UUID::ToString(UUID::GenerateRandomUUID())));
	const string &cache_dir = scoped_dir.GetPath();
	const string remote = "https://example.com/remap-discard.parquet";

	const idx_t old_bs = 4096;
	const idx_t new_bs = 8192;
	auto config = MakeConfig(cache_dir, old_bs);

	// Write only the second half of an 8192-byte block — no first half [0, 4096).
	auto bytes = PatternBytes(old_bs, 'D');
	WriteCacheFile(cache_dir, remote, /*start_offset=*/old_bs, bytes, "v1", config);
	REQUIRE(CacheFileExists(cache_dir, remote, old_bs, old_bs));

	DiskCacheUtil::RemapOnDiskCacheEntriesAfterBlockSizeChange(old_bs, new_bs, config);

	// Old file must be gone.
	REQUIRE_FALSE(CacheFileExists(cache_dir, remote, old_bs, old_bs));
	// No new file: the orphaned second-half block doesn't align to the new 8192-byte grid.
	REQUIRE_FALSE(CacheFileExists(cache_dir, remote, 0, new_bs));
	REQUIRE_FALSE(CacheFileExists(cache_dir, remote, old_bs, new_bs));
}

// Case 2: Two contiguous small files are merged into one larger file (block size increase).
//
// Setup: old_block_size=4096, new_block_size=8192.
//   Write blocks [0, 4096) and [4096, 8192).
// Expected: one merged file [0, 8192) with combined content; both old files deleted.
TEST_CASE("RemapOnDiskCacheEntries: two contiguous small files merged into one large file", "[disk cache remap]") {
	ScopedDirectory scoped_dir(
	    StringUtil::Format("/tmp/test_disk_cache_remap_merge_%s", UUID::ToString(UUID::GenerateRandomUUID())));
	const string &cache_dir = scoped_dir.GetPath();
	const string remote = "https://example.com/remap-merge.parquet";

	const idx_t old_bs = 4096;
	const idx_t new_bs = 8192;
	auto config = MakeConfig(cache_dir, old_bs);

	auto part0 = PatternBytes(old_bs, 'm');
	auto part1 = PatternBytes(old_bs, 'n');
	vector<char> combined = part0;
	combined.insert(combined.end(), part1.begin(), part1.end());

	WriteCacheFile(cache_dir, remote, 0, part0, "v1", config);
	WriteCacheFile(cache_dir, remote, old_bs, part1, "v1", config);
	REQUIRE(CacheFileExists(cache_dir, remote, 0, old_bs));
	REQUIRE(CacheFileExists(cache_dir, remote, old_bs, old_bs));

	DiskCacheUtil::RemapOnDiskCacheEntriesAfterBlockSizeChange(old_bs, new_bs, config);

	// Old files must be gone.
	REQUIRE_FALSE(CacheFileExists(cache_dir, remote, 0, old_bs));
	REQUIRE_FALSE(CacheFileExists(cache_dir, remote, old_bs, old_bs));

	// New merged file [0, 8192) must exist with combined content.
	REQUIRE(CacheFileExists(cache_dir, remote, 0, new_bs));
	auto got = ReadCacheFile(cache_dir, remote, 0, new_bs);
	REQUIRE(got == combined);
}

// Case 3: One large file is split into multiple smaller files (block size decrease).
//
// Setup: old_block_size=8192, new_block_size=4096.
//   Write one block [0, 8192).
// Expected: two files [0, 4096) and [4096, 8192) with correct content; old file deleted.
TEST_CASE("RemapOnDiskCacheEntries: one large file split into multiple smaller files", "[disk cache remap]") {
	ScopedDirectory scoped_dir(
	    StringUtil::Format("/tmp/test_disk_cache_remap_split_%s", UUID::ToString(UUID::GenerateRandomUUID())));
	const string &cache_dir = scoped_dir.GetPath();
	const string remote = "https://example.com/remap-split.parquet";

	const idx_t old_bs = 8192;
	const idx_t new_bs = 4096;
	auto config = MakeConfig(cache_dir, old_bs);

	auto bytes = PatternBytes(old_bs, 's');

	WriteCacheFile(cache_dir, remote, 0, bytes, "v1", config);
	REQUIRE(CacheFileExists(cache_dir, remote, 0, old_bs));

	DiskCacheUtil::RemapOnDiskCacheEntriesAfterBlockSizeChange(old_bs, new_bs, config);

	// Old file must be gone.
	REQUIRE_FALSE(CacheFileExists(cache_dir, remote, 0, old_bs));

	// Two new files must exist with correct content.
	REQUIRE(CacheFileExists(cache_dir, remote, 0, new_bs));
	REQUIRE(CacheFileExists(cache_dir, remote, new_bs, new_bs));

	auto got0 = ReadCacheFile(cache_dir, remote, 0, new_bs);
	auto got1 = ReadCacheFile(cache_dir, remote, new_bs, new_bs);
	REQUIRE(got0 == vector<char>(bytes.begin(), bytes.begin() + static_cast<ptrdiff_t>(new_bs)));
	REQUIRE(got1 == vector<char>(bytes.begin() + static_cast<ptrdiff_t>(new_bs), bytes.end()));
}

// Case 4: Last cache block is shorter than old block size (EOF) and is preserved after remapping.
//
// Setup: old_block_size=8192, new_block_size=4096.
//   Write one "EOF" block [0, 6000): the remote file is 6000 bytes, shorter than old_block_size.
// Expected: two new files [0, 4096) and [4096, 1904); the short tail is kept because it reached EOF.
//   Old file deleted.
TEST_CASE("RemapOnDiskCacheEntries: last block shorter than old block size (EOF) is preserved", "[disk cache remap]") {
	ScopedDirectory scoped_dir(
	    StringUtil::Format("/tmp/test_disk_cache_remap_eof_%s", UUID::ToString(UUID::GenerateRandomUUID())));
	const string &cache_dir = scoped_dir.GetPath();
	const string remote = "https://example.com/remap-eof.parquet";

	const idx_t old_bs = 8192;
	const idx_t new_bs = 4096;
	const idx_t file_size = 6000;               // shorter than old_bs — simulates an EOF block
	const idx_t tail_size = file_size - new_bs; // 1904 bytes
	auto config = MakeConfig(cache_dir, old_bs);

	auto bytes = PatternBytes(file_size, 'e');

	WriteCacheFile(cache_dir, remote, 0, bytes, "v1", config);
	REQUIRE(CacheFileExists(cache_dir, remote, 0, file_size));

	DiskCacheUtil::RemapOnDiskCacheEntriesAfterBlockSizeChange(old_bs, new_bs, config);

	// Old EOF block must be gone.
	REQUIRE_FALSE(CacheFileExists(cache_dir, remote, 0, file_size));

	// Full first new block [0, 4096) must exist.
	REQUIRE(CacheFileExists(cache_dir, remote, 0, new_bs));
	auto got0 = ReadCacheFile(cache_dir, remote, 0, new_bs);
	REQUIRE(got0 == vector<char>(bytes.begin(), bytes.begin() + static_cast<ptrdiff_t>(new_bs)));

	// Short tail [4096, 6000) must be preserved.
	REQUIRE(CacheFileExists(cache_dir, remote, new_bs, tail_size));
	auto got1 = ReadCacheFile(cache_dir, remote, new_bs, tail_size);
	REQUIRE(got1 == vector<char>(bytes.begin() + static_cast<ptrdiff_t>(new_bs), bytes.end()));
}

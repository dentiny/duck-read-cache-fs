#include "catch/catch.hpp"

#include "in_mem_cache_block.hpp"
#include "in_mem_cache_data_entry.hpp"
#include "in_mem_cache_remap.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "page_aligned_data_chunk.hpp"

#include <algorithm>
#include <cstring>
#include <tuple>
#include <utility>

using namespace duckdb; // NOLINT

namespace {

const char *TEST_PATH = "/test/remap_data.bin";

shared_ptr<InMemCacheDataEntry> MakeEntry(const vector<char> &bytes, const string &version_tag = {}) {
	auto entry = make_shared_ptr<InMemCacheDataEntry>();
	entry->data = AllocatePageAlignedChunk(bytes.size());
	std::memcpy(entry->data.data(), bytes.data(), bytes.size());
	entry->data.length = bytes.size();
	entry->version_tag = version_tag;
	return entry;
}

vector<char> PatternBytes(idx_t len, char seed) {
	vector<char> out(len);
	for (idx_t i = 0; i < len; i++) {
		out[i] = static_cast<char>(seed + static_cast<char>(i % 23));
	}
	return out;
}

void SortByFileAndOffset(vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> &out) {
	std::sort(out.begin(), out.end(),
	          [](const auto &a, const auto &b) { return a.first.start_off < b.first.start_off; });
}

void SortByFileThenOffset(vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> &out) {
	std::sort(out.begin(), out.end(), [](const auto &a, const auto &b) {
		return std::tie(a.first.fname, a.first.start_off) < std::tie(b.first.fname, b.first.start_off);
	});
}

void ExpectContiguousBytes(const vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> &sorted,
                           const vector<char> &expected) {
	idx_t off = 0;
	for (const auto &kv : sorted) {
		REQUIRE(kv.first.start_off == off);
		REQUIRE(kv.second);
		REQUIRE(kv.second->data.length == kv.first.blk_size);
		REQUIRE(kv.first.blk_size + off <= expected.size());
		REQUIRE(std::memcmp(kv.second->data.data(), expected.data() + off, kv.first.blk_size) == 0);
		off += kv.first.blk_size;
	}
	REQUIRE(off == expected.size());
}

} // namespace

TEST_CASE("RemapInMemCacheEntries: larger old blocks split on smaller aligned grid", "[data cache remap]") {
	// One 8192-byte old block; new grid 4096 -> two blocks [0,4096) and [4096,8192).
	const idx_t old_len = 8192;
	const idx_t new_bs = 4096;
	auto bytes = PatternBytes(old_len, 'a');

	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken;
	taken.emplace_back(InMemCacheBlock(TEST_PATH, 0, old_len), MakeEntry(bytes, "v1"));

	auto out = RemapInMemCacheEntries(std::move(taken), new_bs);
	REQUIRE(out.size() == 2);
	SortByFileAndOffset(out);
	ExpectContiguousBytes(out, bytes);
	REQUIRE(out[0].second->version_tag == "v1");
	REQUIRE(out[1].second->version_tag == "v1");
}

TEST_CASE("RemapInMemCacheEntries: smaller old blocks merge on larger aligned grid", "[data cache remap]") {
	// Two contiguous 4096-byte blocks, same tag; new grid 8192 -> one block [0,8192).
	const idx_t half = 4096;
	vector<char> part0 = PatternBytes(half, 'm');
	vector<char> part1 = PatternBytes(half, 'n');
	vector<char> full = part0;
	full.insert(full.end(), part1.begin(), part1.end());

	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken;
	taken.emplace_back(InMemCacheBlock(TEST_PATH, 0, half), MakeEntry(part0, "v1"));
	taken.emplace_back(InMemCacheBlock(TEST_PATH, half, half), MakeEntry(part1, "v1"));

	auto out = RemapInMemCacheEntries(std::move(taken), /*new_block_size=*/8192);
	REQUIRE(out.size() == 1);
	REQUIRE(out[0].first.start_off == 0);
	REQUIRE(out[0].first.blk_size == 8192);
	REQUIRE(out[0].second->data.length == 8192);
	REQUIRE(std::memcmp(out[0].second->data.data(), full.data(), 8192) == 0);
	REQUIRE(out[0].second->version_tag == "v1");
}

TEST_CASE("RemapInMemCacheEntries: old span not aligned to new block size (short tail)", "[data cache remap]") {
	// True EOF at 1000 bytes: new grid 512 -> [0,512) and [512,1000) — last piece shorter than 512.
	const idx_t file_len = 1000;
	const idx_t old_len = 1000;
	const idx_t new_bs = 512;
	auto bytes = PatternBytes(old_len, 't');
	unordered_map<string, idx_t> file_sizes;
	file_sizes[string(TEST_PATH)] = file_len;

	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken;
	taken.emplace_back(InMemCacheBlock(TEST_PATH, 0, old_len), MakeEntry(bytes, "v1"));

	auto out = RemapInMemCacheEntries(std::move(taken), new_bs, file_sizes);
	REQUIRE(out.size() == 2);
	SortByFileAndOffset(out);
	REQUIRE(out[0].first.start_off == 0);
	REQUIRE(out[0].first.blk_size == 512);
	REQUIRE(out[1].first.start_off == 512);
	REQUIRE(out[1].first.blk_size == 488);
	ExpectContiguousBytes(out, bytes);
}

TEST_CASE("RemapInMemCacheEntries: file length 1024 but cached prefix is 1000 bytes", "[data cache remap]") {
	// Remote file is 1024 bytes; cache [0,1000). At new_bs=512 only a full [0,512) read is reproducible; [512,1000)
	// is not a complete second read (needs 512 bytes to EOF) — dropped.
	const idx_t file_len = 1024;
	const idx_t old_len = 1000;
	const idx_t new_bs = 512;
	auto bytes = PatternBytes(old_len, 'u');
	vector<char> reused(bytes.begin(), bytes.begin() + static_cast<ptrdiff_t>(new_bs));
	unordered_map<string, idx_t> file_sizes;
	file_sizes[string(TEST_PATH)] = file_len;

	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken;
	taken.emplace_back(InMemCacheBlock(TEST_PATH, 0, old_len), MakeEntry(bytes, "v1"));

	auto out = RemapInMemCacheEntries(std::move(taken), new_bs, file_sizes);
	REQUIRE(out.size() == 1);
	REQUIRE(out[0].first.start_off == 0);
	REQUIRE(out[0].first.blk_size == new_bs);
	REQUIRE(out[0].second->data.length == new_bs);
	ExpectContiguousBytes(out, reused);
}

TEST_CASE("RemapInMemCacheEntries: 1024-byte file fully cached splits evenly on 512 grid", "[data cache remap]") {
	// Full file in one chunk: old_len = file_len = 1024, new grid 512 ⇒ two blocks of exactly 512 bytes.
	const idx_t old_len = 1024;
	const idx_t new_bs = 512;
	auto bytes = PatternBytes(old_len, 'v');
	unordered_map<string, idx_t> file_sizes;
	file_sizes[string(TEST_PATH)] = old_len;

	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken;
	taken.emplace_back(InMemCacheBlock(TEST_PATH, 0, old_len), MakeEntry(bytes, "v1"));

	auto out = RemapInMemCacheEntries(std::move(taken), new_bs, file_sizes);
	REQUIRE(out.size() == 2);
	SortByFileAndOffset(out);
	REQUIRE(out[0].first.start_off == 0);
	REQUIRE(out[0].first.blk_size == 512);
	REQUIRE(out[1].first.start_off == 512);
	REQUIRE(out[1].first.blk_size == 512);
	ExpectContiguousBytes(out, bytes);
}

TEST_CASE("RemapInMemCacheEntries: two small blocks merge onto larger new block with remote EOF",
          "[data cache remap]") {
	// Two 512-byte blocks [0,512) and [512,512); file size 1024; new block size 1000.
	// Like sequential remote reads: first chunk 1000 bytes, then tail 24 bytes.
	const idx_t piece = 512;
	const idx_t file_len = 1024;
	const idx_t new_bs = 1000;
	vector<char> part0 = PatternBytes(piece, 'p');
	vector<char> part1 = PatternBytes(piece, 'q');
	vector<char> full = part0;
	full.insert(full.end(), part1.begin(), part1.end());
	REQUIRE(full.size() == file_len);

	unordered_map<string, idx_t> file_sizes;
	file_sizes[string(TEST_PATH)] = file_len;

	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken;
	taken.emplace_back(InMemCacheBlock(TEST_PATH, 0, piece), MakeEntry(part0, "v1"));
	taken.emplace_back(InMemCacheBlock(TEST_PATH, piece, piece), MakeEntry(part1, "v1"));

	auto out = RemapInMemCacheEntries(std::move(taken), new_bs, file_sizes);
	REQUIRE(out.size() == 2);
	SortByFileAndOffset(out);
	REQUIRE(out[0].first.start_off == 0);
	REQUIRE(out[0].first.blk_size == 1000);
	REQUIRE(out[1].first.start_off == 1000);
	REQUIRE(out[1].first.blk_size == 24);
	ExpectContiguousBytes(out, full);
}

TEST_CASE("RemapInMemCacheEntries: multiple files remapped independently", "[data cache remap]") {
	const char *PATH_A = "/test/multi_file_a.bin";
	const char *PATH_B = "/test/multi_file_b.bin";
	const idx_t new_bs = 512;

	// File A: 1024 bytes fully cached (EOF); expect two full new_bs blocks.
	const idx_t file_a_len = 1024;
	const idx_t cached_a_len = file_a_len;
	auto bytes_a = PatternBytes(cached_a_len, 'A');
	// File B: 2048-byte remote file, only [0, 1000) cached — one full new_bs reuse, tail dropped.
	const idx_t file_b_len = 2048;
	const idx_t cached_b_len = 1000;
	auto bytes_b = PatternBytes(cached_b_len, 'B');
	vector<char> expected_b_reuse(bytes_b.begin(), bytes_b.begin() + static_cast<ptrdiff_t>(new_bs));

	unordered_map<string, idx_t> file_sizes;
	file_sizes[string(PATH_A)] = file_a_len;
	file_sizes[string(PATH_B)] = file_b_len;

	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken;
	taken.emplace_back(InMemCacheBlock(PATH_A, 0, cached_a_len), MakeEntry(bytes_a, "multi"));
	taken.emplace_back(InMemCacheBlock(PATH_B, 0, cached_b_len), MakeEntry(bytes_b, "multi"));

	auto out = RemapInMemCacheEntries(std::move(taken), new_bs, file_sizes);
	REQUIRE(out.size() == 3);
	SortByFileThenOffset(out);

	REQUIRE(out[0].first.fname == string(PATH_A));
	REQUIRE(out[0].first.start_off == 0);
	REQUIRE(out[0].first.blk_size == new_bs);
	REQUIRE(std::memcmp(out[0].second->data.data(), bytes_a.data(), new_bs) == 0);

	REQUIRE(out[1].first.fname == string(PATH_A));
	REQUIRE(out[1].first.start_off == new_bs);
	REQUIRE(out[1].first.blk_size == new_bs);
	REQUIRE(std::memcmp(out[1].second->data.data(), bytes_a.data() + new_bs, new_bs) == 0);

	REQUIRE(out[2].first.fname == string(PATH_B));
	REQUIRE(out[2].first.start_off == 0);
	REQUIRE(out[2].first.blk_size == new_bs);
	REQUIRE(std::memcmp(out[2].second->data.data(), expected_b_reuse.data(), new_bs) == 0);

	REQUIRE(out[0].second->version_tag == "multi");
	REQUIRE(out[1].second->version_tag == "multi");
	REQUIRE(out[2].second->version_tag == "multi");
}

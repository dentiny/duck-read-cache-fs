#include "in_mem_cache_remap.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "page_aligned_data_chunk.hpp"

#include <cstring>

namespace duckdb {

namespace {

struct CachedSlice {
	idx_t byte_len = 0;
	shared_ptr<InMemCacheDataEntry> entry;
};

struct Segment {
	idx_t start = 0;
	idx_t byte_len = 0;
	shared_ptr<InMemCacheDataEntry> entry;
};

idx_t AlignUpToBlock(idx_t offset, idx_t block) {
	D_ASSERT(block > 0);
	idx_t aligned = (offset / block) * block;
	if (aligned < offset) {
		aligned += block;
	}
	return aligned;
}

void CopyRange(const vector<Segment> &segs, idx_t global_start, idx_t copy_len, char *dest) {
	idx_t done = 0;
	while (done < copy_len) {
		const idx_t pos = global_start + done;
		const Segment *chosen = nullptr;
		for (const auto &seg : segs) {
			if (pos >= seg.start && pos < seg.start + seg.byte_len) {
				chosen = &seg;
				break;
			}
		}
		D_ASSERT(chosen != nullptr);
		const idx_t inner = pos - chosen->start;
		const idx_t chunk = MinValue<idx_t>(chosen->byte_len - inner, copy_len - done);
		memcpy(dest + done, chosen->entry->data.data() + inner, chunk);
		done += chunk;
	}
}

unordered_map<string, map<idx_t, CachedSlice>>
BuildFileIndex(vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken) {
	unordered_map<string, map<idx_t, CachedSlice>> by_file;
	for (auto &kv : taken) {
		const auto &key = kv.first;
		const auto &val = kv.second;
		if (!val || val->data.empty() || val->data.length != key.blk_size) {
			continue;
		}
		const auto inserted =
		    by_file[key.fname].emplace(key.start_off, CachedSlice {/*byte_len=*/key.blk_size, /*entry=*/val});
		if (!inserted.second) {
			continue;
		}
	}
	return by_file;
}

void EmitRunOnNewGrid(const string &fname, idx_t run_start, idx_t run_end, const vector<Segment> &run_segments,
                      const string &run_tag, idx_t new_block_size,
                      vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> &out) {
	for (idx_t B = AlignUpToBlock(run_start, new_block_size); B < run_end;) {
		const idx_t piece_len = MinValue<idx_t>(new_block_size, run_end - B);
		auto chunk = AllocatePageAlignedChunk(piece_len);
		CopyRange(run_segments, B, piece_len, chunk.data());
		chunk.length = piece_len;
		auto new_entry = make_shared_ptr<InMemCacheDataEntry>();
		new_entry->data = std::move(chunk);
		new_entry->version_tag = run_tag;
		out.emplace_back(InMemCacheBlock {fname, B, piece_len}, std::move(new_entry));
		B = AlignUpToBlock(B + piece_len, new_block_size);
	}
}

void RemapOneFile(const string &fname, const map<idx_t, CachedSlice> &blk_map, idx_t new_block_size,
                  vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> &out) {
	vector<Segment> run_segments;
	idx_t run_start = 0;
	idx_t run_end = 0;
	bool in_run = false;
	string run_tag;

	auto flush_run = [&]() {
		if (!in_run) {
			return;
		}
		EmitRunOnNewGrid(fname, run_start, run_end, run_segments, run_tag, new_block_size, out);
		run_segments.clear();
		in_run = false;
	};

	auto begin_run = [&](idx_t s, idx_t len, const shared_ptr<InMemCacheDataEntry> &entry) {
		in_run = true;
		run_start = s;
		run_end = s + len;
		run_tag = entry->version_tag;
		run_segments.clear();
		run_segments.push_back(Segment {s, len, entry});
	};

	for (const auto &off_and_slice : blk_map) {
		const idx_t s = off_and_slice.first;
		const idx_t len = off_and_slice.second.byte_len;
		const auto &entry = off_and_slice.second.entry;
		if (len == 0 || !entry) {
			continue;
		}
		if (!in_run) {
			begin_run(s, len, entry);
			continue;
		}
		if (s < run_end) {
			continue;
		}
		if (s == run_end && entry->version_tag == run_tag) {
			run_end = s + len;
			run_segments.push_back(Segment {s, len, entry});
			continue;
		}
		flush_run();
		begin_run(s, len, entry);
	}
	flush_run();
}

} // namespace

vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>>
RemapInMemCacheEntries(vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken,
                       idx_t new_block_size) {
	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> out;
	if (new_block_size == 0) {
		return out;
	}
	auto by_file = BuildFileIndex(std::move(taken));
	for (auto &file_and_blocks : by_file) {
		RemapOneFile(file_and_blocks.first, file_and_blocks.second, new_block_size, out);
	}
	return out;
}

} // namespace duckdb

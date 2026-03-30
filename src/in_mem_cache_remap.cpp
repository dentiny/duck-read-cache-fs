#include "in_mem_cache_remap.hpp"

#include "duckdb/common/helper.hpp"
#include "page_aligned_data_chunk.hpp"

#include <cstring>
#include <map>
#include <unordered_map>

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

static idx_t AlignUpToBlock(idx_t offset, idx_t block) {
	D_ASSERT(block > 0);
	idx_t aligned = (offset / block) * block;
	if (aligned < offset) {
		aligned += block;
	}
	return aligned;
}

static void CopyRange(const vector<Segment> &segs, idx_t global_start, idx_t copy_len, char *dest) {
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

} // namespace

vector<pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>>
RemapInMemCacheEntries(vector<pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken,
                       idx_t new_block_size) {
	vector<pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> out;
	if (new_block_size == 0) {
		return out;
	}

	unordered_map<string, map<idx_t, CachedSlice>> by_file;
	for (auto &kv : taken) {
		const auto &key = kv.first;
		const auto &val = kv.second;
		if (!val || val->data.empty() || val->data.length != key.blk_size) {
			continue;
		}
		const auto inserted = by_file[key.fname].emplace(
		    key.start_off, CachedSlice {/*byte_len=*/key.blk_size, /*entry=*/val});
		if (!inserted.second) {
			continue;
		}
	}

	for (auto &file_and_blocks : by_file) {
		const string &fname = file_and_blocks.first;
		auto &blk_map = file_and_blocks.second;

		vector<Segment> run_segments;
		idx_t run_start = 0;
		idx_t run_end = 0;
		bool in_run = false;
		string run_tag;

		auto flush_run = [&]() {
			if (!in_run) {
				return;
			}
			for (idx_t B = AlignUpToBlock(run_start, new_block_size); B < run_end; B += new_block_size) {
				const idx_t piece_len = MinValue<idx_t>(new_block_size, run_end - B);
				auto chunk = AllocatePageAlignedChunk(piece_len);
				CopyRange(run_segments, B, piece_len, chunk.data());
				chunk.length = piece_len;
				auto new_entry = make_shared_ptr<InMemCacheDataEntry>();
				new_entry->data = std::move(chunk);
				new_entry->version_tag = run_tag;
				out.emplace_back(InMemCacheBlock {fname, B, piece_len}, std::move(new_entry));
			}
			run_segments.clear();
			in_run = false;
		};

		for (auto it = blk_map.begin(); it != blk_map.end(); ++it) {
			const idx_t s = it->first;
			const idx_t len = it->second.byte_len;
			const auto &entry = it->second.entry;
			if (len == 0 || !entry) {
				continue;
			}
			if (!in_run) {
				in_run = true;
				run_start = s;
				run_end = s + len;
				run_tag = entry->version_tag;
				run_segments.push_back(Segment {s, len, entry});
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
			in_run = true;
			run_start = s;
			run_end = s + len;
			run_tag = entry->version_tag;
			run_segments.push_back(Segment {s, len, entry});
		}
		flush_run();
	}

	return out;
}

} // namespace duckdb

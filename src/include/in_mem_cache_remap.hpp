// Remap in-memory data cache entries onto a new block size grid after Take().

#pragma once

#include <utility>

#include "in_mem_cache_block.hpp"
#include "in_mem_cache_data_entry.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// [file_size_by_path]: optional remote file sizes (bytes). When present, caps emission at EOF like a real read.
// If a contiguous run ends before EOF (prefix cache), only full new_block_size-aligned chunks wholly inside the
// cached range are reused; any trailing bytes that are not a complete new read are dropped (same as not having
// cached them after a resize). When the run reaches EOF (run_end == file_size), short final blocks are kept.
vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>>
RemapInMemCacheEntries(vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken, idx_t new_block_size,
                       const unordered_map<string, idx_t> &file_size_by_path = {});

} // namespace duckdb

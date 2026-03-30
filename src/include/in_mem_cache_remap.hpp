// Remap in-memory data cache entries onto a new block size grid after Take().

#pragma once

#include "in_mem_cache_block.hpp"
#include "in_mem_cache_data_entry.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

vector<pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>>
RemapInMemCacheEntries(vector<pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> taken,
                       idx_t new_block_size);

} // namespace duckdb

#include "in_mem_cache_block.hpp"

#include "utils/include/url_utils.hpp"

namespace duckdb {

InMemCacheBlock::InMemCacheBlock(const string &path, idx_t start_off_p, idx_t blk_size_p)
    : fname(URLUtils::StripQueryAndFragment(path)), start_off(start_off_p), blk_size(blk_size_p) {
}

} // namespace duckdb

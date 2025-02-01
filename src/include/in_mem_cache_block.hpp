// In-memory cache cache block key.

#pragma once

#include <string>
#include <tuple>
#include <cstdint>
#include <functional>

#include "duckdb/common/string_util.hpp"

namespace duckdb {

// TODO(hjiang): Use customized hash and equal functor to store in cache,
// temporarily unblock by using string as key.
struct InMemCacheBlock {
  std::string fname;
  uint64_t start_off = 0;
  uint64_t blk_size = 0;

  string ToString() const {
    return StringUtil::Format("%s-%llu-%llu", fname, start_off, blk_size);
  }
};

} // namespace duckdb

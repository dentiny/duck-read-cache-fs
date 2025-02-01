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
};

struct InMemCacheBlockEqual {
  bool operator()(const InMemCacheBlock &lhs,
                  const InMemCacheBlock &rhs) const {
    return std::tie(lhs.fname, lhs.start_off, lhs.blk_size) ==
           std::tie(rhs.fname, rhs.start_off, rhs.blk_size);
  }
};
struct InMemCacheBlockHash {
  std::size_t operator()(const InMemCacheBlock &key) const {
    return std::hash<std::string>{}(key.fname) ^
           std::hash<uint64_t>{}(key.start_off) ^
           std::hash<uint64_t>{}(key.blk_size);
  }
};

} // namespace duckdb

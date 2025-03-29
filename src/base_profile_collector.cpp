#include "base_profile_collector.hpp"

namespace duckdb {
const std::array<const char *, BaseProfileCollector::kIoOperationCount> BaseProfileCollector::OPER_NAMES = {
    "open",
    "read",
    "glob",
};

// Cache entity name, indexed by cache entity enum.
const std::array<const char *, BaseProfileCollector::kCacheEntityCount> BaseProfileCollector::CACHE_ENTITY_NAMES = {
    "metadata",
    "data",
    "file handle",
    "glob",
};
} // namespace duckdb

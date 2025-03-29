#include "base_profile_collector.hpp"

namespace duckdb {
const NoDestructor<vector<std::string>> BaseProfileCollector::OPER_NAMES {
    "open",
    "read",
    "glob",
};

// Cache entity name, indexed by cache entity enum.
const NoDestructor<vector<std::string>> BaseProfileCollector::CACHE_ENTITY_NAMES {
    "metadata",
    "data",
    "file handle",
    "glob",
};
} // namespace duckdb

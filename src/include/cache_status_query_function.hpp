// Function which queries cache status.

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Get the table function to query cache status.
TableFunction GetDataCacheStatusQueryFunc();

// Get the table function to query cache access status.
TableFunction GetCacheAccessInfoQueryFunc();

// Get the table function to query wrapped cache filesystems.
TableFunction GetWrappedCacheFileSystemsFunc();

} // namespace duckdb

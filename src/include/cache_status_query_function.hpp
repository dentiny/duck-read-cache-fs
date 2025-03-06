// Function which queries cache status.

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Get the table function to query cache status.
TableFunction GetCacheStatusQueryFunc();

} // namespace duckdb

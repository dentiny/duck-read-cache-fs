// Util functions for cache exclusion functions.

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Scalar function to add exclusion regex.
void AddCacheExclusionRegex(const DataChunk &args, ExpressionState &state, Vector &result);

// Scalar function to reset exclusion regex.
void ResetCacheExclusionRegex(const DataChunk &args, ExpressionState &state, Vector &result);

// Table function to list all exclusion regex.
TableFunction ListCacheExclusionRegex();

} // namespace duckdb

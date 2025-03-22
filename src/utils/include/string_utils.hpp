// String related operations.

#pragma once

#include <string>

namespace duckdb {

// Return whether the given [expr] is a glob expression.
bool IsGlobExpression(const std::string &expr);

} // namespace duckdb

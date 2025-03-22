#include "string_utils.hpp"

namespace duckdb {

// Reference: https://en.wikipedia.org/wiki/Glob_(programming)
bool IsGlobExpression(const std::string &expr) {
	return expr.find_first_of("[]*?") != std::string::npos;
}

} // namespace duckdb

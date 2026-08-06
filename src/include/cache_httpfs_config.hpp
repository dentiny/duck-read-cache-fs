#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

// Forward declaration
class DatabaseInstance;

void SetCacheHttpfsExtensionOption(DatabaseInstance &instance, const string &name, Value value);

} // namespace duckdb

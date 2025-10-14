// Functions which query duckdb filesystem status.

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// List the registered filesystem instances.
TableFunction ListRegisteredFileSystemsQueryFunc();

} // namespace duckdb

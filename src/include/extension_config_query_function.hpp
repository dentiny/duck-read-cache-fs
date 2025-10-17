// This file contains query function to get current cache config.

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Get table function to query data cache config.
TableFunction GetDataCacheConfigQueryFunc();

// Get table function to query metadata cache config.
TableFunction GetMetadataCacheConfigQueryFunc();

// Get table function to query file handle cache config.
TableFunction GetFileHandleCacheConfigQueryFunc();

// Get table function to query glob cache config.
TableFunction GetGlobCacheConfigQueryFunc();

// Get table function to query cache type and enablement.
TableFunction GetCacheTypeQueryFunc();

// Get table function to query cache extension config.
TableFunction GetCacheConfigQueryFunc();

} // namespace duckdb

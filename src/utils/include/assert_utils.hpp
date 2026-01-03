// Utils used for assertions, which takes affect for all build modes.
// In constrast, D_ASSERT only works under debug build.

#pragma once

// Force assertion implementation, which always asserts whatever build type is used.
// TODO(hjiang): Could be removed after upgrade to v1.5.0 
#define CACHE_HTTPFS_ALWAYS_ASSERT(condition) duckdb::DuckDBAssertInternal(bool(condition), #condition, __FILE__, __LINE__)

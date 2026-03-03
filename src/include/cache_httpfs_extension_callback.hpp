//===----------------------------------------------------------------------===//
// Extension callback for cache_httpfs connection lifecycle management
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/extension_callback.hpp"

namespace duckdb {

class CacheHttpfsExtensionCallback : public ExtensionCallback {
public:
	void OnConnectionClosed(ClientContext &context) override;
};

} // namespace duckdb

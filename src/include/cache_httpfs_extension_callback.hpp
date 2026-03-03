//===----------------------------------------------------------------------===//
// Extension callback for cache_httpfs connection lifecycle management
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/planner/extension_callback.hpp"

namespace duckdb {

// Forward declaration.
struct CacheHttpfsInstanceState;

class CacheHttpfsExtensionCallback : public ExtensionCallback {
public:
	CacheHttpfsExtensionCallback(weak_ptr<CacheHttpfsInstanceState> instance_state, connection_t connection_id);
	~CacheHttpfsExtensionCallback() override;
	void OnConnectionClosed(ClientContext &context) override;

private:
	weak_ptr<CacheHttpfsInstanceState> instance_state;
	connection_t connection_id;
};

} // namespace duckdb

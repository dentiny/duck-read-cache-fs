#include "cache_httpfs_extension_callback.hpp"

#include "cache_httpfs_instance_state.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void CacheHttpfsExtensionCallback::OnConnectionClosed(ClientContext &context) {
	// Remove the profile collector for this connection when it's closed
	auto instance_state = GetInstanceStateShared(*context.db);
	if (instance_state) {
		auto conn_id = context.GetConnectionId();
		instance_state->profile_collector_manager.RemoveProfileCollector(conn_id);
	}
}

} // namespace duckdb

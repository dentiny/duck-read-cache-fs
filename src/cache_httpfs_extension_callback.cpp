#include "cache_httpfs_extension_callback.hpp"

#include "cache_httpfs_instance_state.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

CacheHttpfsExtensionCallback::CacheHttpfsExtensionCallback(weak_ptr<CacheHttpfsInstanceState> instance_state_p,
                                                           connection_t connection_id_p)
    : instance_state(std::move(instance_state_p)), connection_id(connection_id_p) {
}

CacheHttpfsExtensionCallback::~CacheHttpfsExtensionCallback() {
}

void CacheHttpfsExtensionCallback::OnConnectionClosed(ClientContext &context) {
	const auto connection_id = context.GetConnectionId();
	auto state = instance_state.lock();
	if (state == nullptr) {
		throw InternalException("cache_httpfs instance state is no longer valid");
	}
	state->profile_collector_manager.RemoveProfileCollector(connection_id);
}

} // namespace duckdb

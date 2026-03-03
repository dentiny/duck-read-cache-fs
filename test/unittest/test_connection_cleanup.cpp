#include "catch/catch.hpp"

#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Multiple connections registered in state and profile manager are all removed when destructed",
          "[connection cleanup]") {
	DuckDB db(nullptr);
	auto &instance = db.instance;

	// Load the cache_httpfs extension to register the connection cleanup callback
	Connection initial_con(db);
	REQUIRE_NO_FAIL(initial_con.Query("LOAD cache_httpfs;"));

	// Get the instance state created by the extension
	auto instance_state = GetInstanceStateShared(*instance);
	REQUIRE(instance_state);

	constexpr size_t num_connections = 5;
	vector<unique_ptr<Connection>> connections;
	connections.reserve(num_connections);

	for (size_t idx = 0; idx < num_connections; ++idx) {
		connections.emplace_back(make_uniq<Connection>(db));
		auto &context = *connections.back()->context;
		
		// Setting profile type will register the connection in the profile manager
		// Cleanup will be handled automatically by CacheHttpfsExtensionCallback::OnConnectionClosed
		auto result = connections.back()->Query("SET cache_httpfs_profile_type = 'noop'");
		REQUIRE(!result->HasError());
	}

	auto &connection_manager = ConnectionManager::Get(*instance);
	// Note: connection count includes the initial_con + num_connections
	REQUIRE(connection_manager.GetConnectionCount() == num_connections + 1);
	REQUIRE(instance_state->profile_collector_manager.GetProfileCollectorCount() == num_connections + 1);

	// Destroy the test connections (but keep initial_con alive)
	connections.clear();
	REQUIRE(connection_manager.GetConnectionCount() == 1); // Only initial_con remains
	REQUIRE(instance_state->profile_collector_manager.GetProfileCollectorCount() == 1);
	
	// Destroy initial_con to clean up everything
	initial_con = Connection(db); // Move-assign to destroy old connection
	REQUIRE(connection_manager.GetConnectionCount() == 1); // New connection
	REQUIRE(instance_state->profile_collector_manager.GetProfileCollectorCount() == 0); // Old one cleaned up
}

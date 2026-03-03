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
	initial_con.Query("LOAD cache_httpfs;");

	// Get the instance state created by the extension
	auto instance_state = GetInstanceStateShared(*instance);
	REQUIRE(instance_state);

	constexpr size_t num_connections = 5;
	vector<unique_ptr<Connection>> connections;
	connections.reserve(num_connections);

	for (size_t idx = 0; idx < num_connections; ++idx) {
		connections.emplace_back(make_uniq<Connection>(db));
		// Setting profile type will register the connection in the profile manager
		auto result = connections.back()->Query("SET cache_httpfs_profile_type = 'temp'");
		REQUIRE(!result->HasError());
	}

	auto &connection_manager = ConnectionManager::Get(*instance);
	// Note: connection count includes the initial_con + num_connections
	REQUIRE(connection_manager.GetConnectionCount() == num_connections + 1);
	// Initial connection is not registered.
	REQUIRE(instance_state->profile_collector_manager.GetProfileCollectorCount() == num_connections);

	// Destroy the test connections
	connections.clear();
	REQUIRE(connection_manager.GetConnectionCount() == 1); // Only initial_con remains
	REQUIRE(instance_state->profile_collector_manager.GetProfileCollectorCount() == 0);
}

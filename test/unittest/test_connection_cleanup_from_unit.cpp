#include "catch/catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database.hpp"
#include "scoped_directory.hpp"

using namespace duckdb; // NOLINT

namespace {

struct ConnectionCleanupFixture {
	ScopedDirectory scoped_dir;
	std::string cache_dir;

	ConnectionCleanupFixture()
	    : scoped_dir(StringUtil::Format("/tmp/duckdb_test_connection_cleanup_%s",
	                                    UUID::ToString(UUID::GenerateRandomUUID()))) {
		cache_dir = StringUtil::Format("%s/cache", scoped_dir.GetPath());
	}
};

} // namespace

TEST_CASE_METHOD(ConnectionCleanupFixture,
                 "Multiple connections registered in state and profile manager are all removed when destructed",
                 "[connection cleanup]") {
	DuckDB db(nullptr);
	auto &instance = db.instance;

	// Set up per-instance state and cache filesystem.
	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	instance_state->config.cache_type = *ON_DISK_CACHE_TYPE;
	SetInstanceState(*instance, instance_state);

	auto local_fs = LocalFileSystem::CreateLocal();
	local_fs->CreateDirectory(cache_dir);

	auto db_instance_state = GetInstanceStateShared(*instance);
	instance->GetFileSystem().RegisterSubSystem(
	    make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal(), db_instance_state));

	constexpr size_t num_connections = 5;
	vector<unique_ptr<Connection>> connections;
	connections.reserve(num_connections);

	for (size_t idx = 0; idx < num_connections; ++idx) {
		connections.emplace_back(make_uniq<Connection>(db));
		auto &context = *connections.back()->context;
		auto conn_id = context.GetConnectionId();
		
		// Directly register the connection into the profile manager and cleanup state
		instance_state->profile_collector_manager.SetProfileCollector(conn_id, "noop");
		RegisterConnectionCleanupState(context, db_instance_state);
	}

	auto &connection_manager = ConnectionManager::Get(*instance);
	REQUIRE(connection_manager.GetConnectionCount() == num_connections);
	REQUIRE(instance_state->profile_collector_manager.GetProfileCollectorCount() == num_connections);

	connections.clear();
	REQUIRE(connection_manager.GetConnectionCount() == 0);
	REQUIRE(instance_state->profile_collector_manager.GetProfileCollectorCount() == 0);
}

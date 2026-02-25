// Unit test for setting extension config.

#include "catch/catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "filesystem_utils.hpp"
#include "in_memory_cache_reader.hpp"
#include "scoped_directory.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

struct SetExtensionConfigFixture {
	ScopedDirectory scoped_dir;
	std::string test_on_disk_cache_directory;
	std::string test_second_on_disk_cache_directory;
	std::string test_on_disk_cache_file;
	SetExtensionConfigFixture()
	    : scoped_dir(
	          StringUtil::Format("/tmp/duckdb_test_extension_config_%s", UUID::ToString(UUID::GenerateRandomUUID()))) {
		test_on_disk_cache_directory = StringUtil::Format("%s/cache", scoped_dir.GetPath());
		test_second_on_disk_cache_directory = StringUtil::Format("%s/cache_second", scoped_dir.GetPath());
		test_on_disk_cache_file = StringUtil::Format("%s/test-config.parquet", scoped_dir.GetPath());
	}
};
} // namespace

TEST_CASE("Test on incorrect config", "[extension config test]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Set non-existent config parameter.
	const std::string some_cache_path = "/tmp/duckdb_test_cache_httpfs_cache";
	auto result = con.Query(StringUtil::Format("SET wrong_cache_httpfs_cache_directory ='%s'", some_cache_path));
	REQUIRE(result->HasError());

	// Set existent config parameter to incorrect type.
	result = con.Query(StringUtil::Format("SET cache_httpfs_cache_block_size='hello'"));
	REQUIRE(result->HasError());
}

TEST_CASE("Test on correct config", "[extension config test]") {
	DuckDB db(nullptr);
	Connection con(db);

	// On-disk cache directory.
	auto result = con.Query(StringUtil::Format("SET cache_httpfs_cache_directory='helloworld'"));
	REQUIRE(!result->HasError());

	// Cache block size.
	result = con.Query(StringUtil::Format("SET cache_httpfs_cache_block_size=10"));
	REQUIRE(!result->HasError());

	// In-memory cache block count.
	result = con.Query(StringUtil::Format("SET cache_httpfs_max_in_mem_cache_block_count=10"));
	REQUIRE(!result->HasError());
}

TEST_CASE_METHOD(SetExtensionConfigFixture, "Test on changing extension config change default cache dir path setting",
                 "[extension config test]") {
	DuckDB db(nullptr);
	auto &instance = db.instance;

	// Set up per-instance state for the extension
	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	instance_state->config.cache_type = *ON_DISK_CACHE_TYPE;
	SetInstanceState(*instance, instance_state);
	InitializeCacheReaderForTest(instance_state, instance_state->config);

	auto local_fs = LocalFileSystem::CreateLocal();
	local_fs->CreateDirectory(test_on_disk_cache_directory);

	auto &fs = instance->GetFileSystem();
	auto db_instance_state = GetInstanceStateShared(*instance);
	fs.RegisterSubSystem(make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal(), db_instance_state));

	Connection con(db);
	con.Query(StringUtil::Format("SET cache_httpfs_cache_directory ='%s'", test_on_disk_cache_directory));
	con.Query("SET cache_httpfs_disk_cache_reader_enable_memory_cache=false");
	con.Query("CREATE TABLE integers AS SELECT i, i+1 as j FROM range(10) r(i)");
	con.Query(StringUtil::Format("COPY integers TO '%s'", test_on_disk_cache_file));

	// Ensure the cache directory is empty before executing the query.
	const int files = GetFileCountUnder(test_on_disk_cache_directory);
	REQUIRE(files == 0);

	// After executing the query, the cache directory should have one cache file.
	auto result = con.Query(StringUtil::Format("SELECT * FROM '%s'", test_on_disk_cache_file));
	REQUIRE(!result->HasError());

	const int files_after_query = GetFileCountUnder(test_on_disk_cache_directory);
	const auto files_in_cache = GetSortedFilesUnder(test_on_disk_cache_directory);
	REQUIRE(files_after_query == 1);

	// Verify cached read still works (should hit the cache)
	result = con.Query(StringUtil::Format("SELECT * FROM '%s'", test_on_disk_cache_file));
	REQUIRE(!result->HasError());

	// Files should remain the same after cached read
	const int files_after_cached_read = GetFileCountUnder(test_on_disk_cache_directory);
	REQUIRE(files_after_cached_read == 1);
}

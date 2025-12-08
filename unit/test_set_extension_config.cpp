// Unit test for setting extension config.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "filesystem_utils.hpp"
#include "in_memory_cache_reader.hpp"
#include "scope_guard.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {
const std::string TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_httpfs_cache";
const std::string TEST_SECOND_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cache_httpfs_cache_second";
const std::string TEST_ON_DISK_CACHE_FILE = "/tmp/test-config.parquet";

void CleanupTestDirectory() {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	if (local_filesystem->DirectoryExists(TEST_ON_DISK_CACHE_DIRECTORY)) {
		local_filesystem->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
	}
	if (local_filesystem->DirectoryExists(TEST_SECOND_ON_DISK_CACHE_DIRECTORY)) {
		local_filesystem->RemoveDirectory(TEST_SECOND_ON_DISK_CACHE_DIRECTORY);
	}
	if (local_filesystem->FileExists(TEST_ON_DISK_CACHE_FILE)) {
		local_filesystem->RemoveFile(TEST_ON_DISK_CACHE_FILE);
	}
}
} // namespace

TEST_CASE("Test on incorrect config", "[extension config test]") {
	SCOPE_EXIT {
		ResetGlobalStateAndConfig();
	};

	DuckDB db(nullptr);
	Connection con(db);

	// Set non-existent config parameter.
	auto result =
	    con.Query(StringUtil::Format("SET wrong_cache_httpfs_cache_directory ='%s'", TEST_ON_DISK_CACHE_DIRECTORY));
	REQUIRE(result->HasError());

	// Set existent config parameter to incorrect type.
	result = con.Query(StringUtil::Format("SET cache_httpfs_cache_block_size='hello'"));
	REQUIRE(result->HasError());
}

TEST_CASE("Test on correct config", "[extension config test]") {
	SCOPE_EXIT {
		ResetGlobalStateAndConfig();
	};

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

TEST_CASE("Test on changing extension config change default cache dir path setting", "[extension config test]") {
	SCOPE_EXIT {
		ResetGlobalStateAndConfig();
	};

	DuckDB db(nullptr);
	auto &instance = db.instance;
	auto &fs = instance->GetFileSystem();
	fs.RegisterSubSystem(make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal()));

	Connection con(db);
	con.Query(StringUtil::Format("SET cache_httpfs_cache_directory ='%s'", TEST_ON_DISK_CACHE_DIRECTORY));
	con.Query("SET cache_httpfs_disk_cache_reader_enable_memory_cache=false");
	con.Query("CREATE TABLE integers AS SELECT i, i+1 as j FROM range(10) r(i)");
	con.Query(StringUtil::Format("COPY integers TO '%s'", TEST_ON_DISK_CACHE_FILE));

	// Ensure the cache directory is empty before executing the query.
	const int files = GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(files == 0);

	// After executing the query, the cache directory should have one cache file.
	auto result = con.Query(StringUtil::Format("SELECT * FROM '%s'", TEST_ON_DISK_CACHE_FILE));
	REQUIRE(!result->HasError());

	const int files_after_query = GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	const auto files_in_cache = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(files_after_query == 1);

	// Change the cache directory path and execute the query again.
	con.Query(StringUtil::Format("SET cache_httpfs_cache_directory ='%s'", TEST_SECOND_ON_DISK_CACHE_DIRECTORY));
	result = con.Query(StringUtil::Format("SELECT * FROM '%s'", TEST_ON_DISK_CACHE_FILE));
	REQUIRE(!result->HasError());

	// After executing the query, the NEW directory should have one cache file.
	// Both directories should have the same cache file.
	const int files_after_query_second = GetFileCountUnder(TEST_SECOND_ON_DISK_CACHE_DIRECTORY);
	const auto files_in_cache_second = GetSortedFilesUnder(TEST_SECOND_ON_DISK_CACHE_DIRECTORY);
	REQUIRE(files_after_query_second == 1);
	REQUIRE(files_in_cache == files_in_cache_second);

	// Update cache type to in-memory cache type and on-disk cache directory, and check no new cache files created.
	//
	// Set cache directory to the root directory, which test program doesn't have the permission to write to.
	con.Query(StringUtil::Format("SET cache_httpfs_cache_directory ='%s'", "/non_existent_directory"));
	con.Query("SET cache_httpfs_type='in_mem'");
	result = con.Query(StringUtil::Format("SELECT * FROM '%s'", TEST_ON_DISK_CACHE_FILE));
	REQUIRE(!result->HasError());
};

int main(int argc, char **argv) {
	CleanupTestDirectory();
	int result = Catch::Session().run(argc, argv);
	CleanupTestDirectory();
	return result;
}

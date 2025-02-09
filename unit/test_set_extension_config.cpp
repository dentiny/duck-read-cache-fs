// Unit test for setting extension config.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "disk_cache_filesystem.hpp"
#include "filesystem_utils.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "cache_filesystem_config.hpp"

#include <algorithm>

using namespace duckdb; // NOLINT

namespace {
const std::string TEST_ON_DISK_CACHE_DIRECTORY =
    "/tmp/duckdb_test_cached_http_cache";
const std::string TEST_SECOND_ON_DISK_CACHE_DIRECTORY =
    "/tmp/duckdb_test_cached_http_cache_second";
const std::string TEST_ON_DISK_CACHE_FILE = "/tmp/test-config.parquet";

} // namespace

TEST_CASE("Test on changing extension config"
          "change defaul cache dir path setting",
          "[extension config test]") {

  DuckDB db(nullptr);
  auto &instance = db.instance;
  auto &fs = instance->GetFileSystem();
  fs.RegisterSubSystem(make_uniq<DiskCacheFileSystem>(
      LocalFileSystem::CreateLocal(), OnDiskCacheConfig{}));

  Connection con(db);
  con.Query("SET cached_http_cache_directory=" +
            StringUtil::Format("'%s'", TEST_ON_DISK_CACHE_DIRECTORY));
  con.Query("CREATE TABLE integers AS SELECT i, i+1 as j FROM range(10) r(i)");
  con.Query("COPY integers TO" +
            StringUtil::Format("'%s'", TEST_ON_DISK_CACHE_FILE));

  // Ensure the cache directory is empty before executing the query.
  int files = GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY);
  REQUIRE(files == 0);

  con.Query("SELECT * FROM" +
            StringUtil::Format("'%s'", TEST_ON_DISK_CACHE_FILE));

  // After executing the query, the cache directory should have one cache file.
  int files_after_query = GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY);
  vector<string> files_in_cache =
      GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
  REQUIRE(files_after_query == 1);

  // Change the cache directory path and execute the query again.
  con.Query("SET cached_http_cache_directory=" +
            StringUtil::Format("'%s'", TEST_SECOND_ON_DISK_CACHE_DIRECTORY));
  con.Query("SELECT * FROM" +
            StringUtil::Format("'%s'", TEST_ON_DISK_CACHE_FILE));

  // After executing the query, the NEW directory should have one cache file.
  // Both directories should have the same cache file.
  const int files_after_query_second =
      GetFileCountUnder(TEST_SECOND_ON_DISK_CACHE_DIRECTORY);
  vector<string> files_in_cache_second =
      GetSortedFilesUnder(TEST_SECOND_ON_DISK_CACHE_DIRECTORY);
  REQUIRE(files_after_query_second == 1);
  REQUIRE(files_in_cache == files_in_cache_second);

  // Clean up
  auto local_filesystem = LocalFileSystem::CreateLocal();
  local_filesystem->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  local_filesystem->RemoveDirectory(TEST_SECOND_ON_DISK_CACHE_DIRECTORY);
  local_filesystem->RemoveFile(TEST_ON_DISK_CACHE_FILE);
};

int main(int argc, char **argv) {
  int result = Catch::Session().run(argc, argv);
  return result;
}

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem_config.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "filesystem_utils.hpp"
#include "scope_guard.hpp"

#include <utime.h>

using namespace duckdb; // NOLINT

namespace {
constexpr uint64_t TEST_FILE_SIZE = 26;
const auto TEST_FILE_CONTENT = []() {
	string content(TEST_FILE_SIZE, '\0');
	for (uint64_t idx = 0; idx < TEST_FILE_SIZE; ++idx) {
		content[idx] = 'a' + idx;
	}
	return content;
}();
const auto TEST_DIRECTORY = "/tmp/duckdb_test_cache";
const auto TEST_FILENAME = StringUtil::Format("/tmp/duckdb_test_cache/%s", UUID::ToString(UUID::GenerateRandomUUID()));
} // namespace

TEST_CASE("Test glob operation", "[cache filesystem test]") {
	SCOPE_EXIT {
		ResetGlobalConfig();
	};
	g_enable_glob_cache = true;

	auto cache_filesystem = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());
	REQUIRE(cache_filesystem->Glob(TEST_FILENAME) == vector<string> {TEST_FILENAME});
	REQUIRE(cache_filesystem->Glob("/tmp/duckdb_test_cache/*") == vector<string> {TEST_FILENAME});
}

int main(int argc, char **argv) {
	// Set global cache type for testing.
	*g_test_cache_type = *ON_DISK_CACHE_TYPE;

	auto local_filesystem = LocalFileSystem::CreateLocal();
	local_filesystem->CreateDirectory(TEST_DIRECTORY);
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();

	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveDirectory(TEST_DIRECTORY);
	return result;
}

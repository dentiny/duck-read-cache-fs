// Unit test to verify IO operation latency recording does reflect in the profile output.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "io_operations.hpp"
#include "scoped_directory.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

constexpr uint64_t TEST_FILE_SIZE = 1024;
const auto TEST_FILE_CONTENT = []() {
	string content(TEST_FILE_SIZE, '\0');
	for (uint64_t idx = 0; idx < TEST_FILE_SIZE; ++idx) {
		content[idx] = 'a' + (idx % 26);
	}
	return content;
}();

void CreateTestFile(const string &filepath, const string &content) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(filepath, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<char *>(content.data()), content.length(), /*location=*/0);
	file_handle->Sync();
	file_handle->Close();
}

bool ProfileContainsOperation(const string &profile, const string &operation_name) {
	// Check if profile contains "{operation_name} operation latency"
	string expected = StringUtil::Format("%s operation latency", operation_name);
	return profile.find(expected) != string::npos;
}

} // namespace

TEST_CASE("Test IO operation latency recording", "[io operation latency]") {
	const auto TEST_DIRECTORY = StringUtil::Format("/tmp/test_io_latency_%s", UUID::ToString(UUID::GenerateRandomUUID()));
	ScopedDirectory scoped_dir(TEST_DIRECTORY);

	const auto TEST_FILENAME_GLOB = StringUtil::Format("%s/*", TEST_DIRECTORY);
	const auto TEST_FILENAME_1 = StringUtil::Format("%s/test_file_1", TEST_DIRECTORY);
	const auto TEST_FILENAME_2 = StringUtil::Format("%s/test_file_2", TEST_DIRECTORY);

	// Create test files
	CreateTestFile(TEST_FILENAME_1, TEST_FILE_CONTENT);
	CreateTestFile(TEST_FILENAME_2, TEST_FILE_CONTENT);

	// Setup cache filesystem with temp profile collector
	TestCacheConfig config;
	config.cache_type = "in_mem";
	config.profile_type = "temp";
	TestCacheFileSystemHelper helper(config);
	auto *cache_filesystem = helper.GetCacheFileSystem();

	// Clear profile to start fresh
	auto &profiler = cache_filesystem->GetProfileCollector();
	profiler.Reset();

	// Perform open operation
	{
		auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME_1, FileOpenFlags::FILE_FLAGS_READ);
		REQUIRE(file_handle != nullptr);
	}
	// Perform read operation
	{
		auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME_1, FileOpenFlags::FILE_FLAGS_READ);
		char buffer[256];
		cache_filesystem->Read(*file_handle, buffer, 256, /*location=*/0);
	}
	// Perform write operation
	{
		auto file_handle = cache_filesystem->OpenFile(TEST_FILENAME_2, FileOpenFlags::FILE_FLAGS_WRITE);
		const char *write_data = "test write data";
		cache_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(write_data)),
		                        strlen(write_data), /*location=*/0);
        cache_filesystem->FileSync(*file_handle);
    }
	// Perform glob operation
    auto open_file_info = cache_filesystem->Glob(TEST_FILENAME_GLOB);
    REQUIRE(open_file_info.size() >= 1);
	// Perform file remove operation
	cache_filesystem->RemoveFile(TEST_FILENAME_2);
	// Perform cache clear operation
	cache_filesystem->ClearCache(TEST_FILENAME_1);

	// Get profile stats and verify operations are recorded
	auto stats_pair = profiler.GetHumanReadableStats();
	const string &profile = stats_pair.first;

	// Verify all operations we performed are present in the profile
	REQUIRE(ProfileContainsOperation(profile, OPER_NAMES[static_cast<idx_t>(IoOperation::kOpen)]));
	REQUIRE(ProfileContainsOperation(profile, OPER_NAMES[static_cast<idx_t>(IoOperation::kRead)]));
	REQUIRE(ProfileContainsOperation(profile, OPER_NAMES[static_cast<idx_t>(IoOperation::kWrite)]));
	REQUIRE(ProfileContainsOperation(profile, OPER_NAMES[static_cast<idx_t>(IoOperation::kFileSync)]));
	REQUIRE(ProfileContainsOperation(profile, OPER_NAMES[static_cast<idx_t>(IoOperation::kFileRemove)]));
	REQUIRE(ProfileContainsOperation(profile, OPER_NAMES[static_cast<idx_t>(IoOperation::kGlob)]));
	REQUIRE(ProfileContainsOperation(profile, OPER_NAMES[static_cast<idx_t>(IoOperation::kFilePathCacheClear)]));
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}

// Unit test for no-op cache filesystem.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "noop_cache_reader.hpp"
#include "scope_guard.hpp"
#include "test_utils.hpp"

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
const auto TEST_FILENAME = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
} // namespace

TEST_CASE("Test on noop cache filesystem", "[noop cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "noop";
	config.cache_block_size = TEST_FILE_SIZE;
	TestCacheFileSystemHelper helper(config);
	auto *noop_filesystem = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = noop_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		noop_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second uncached read.
	{
		auto handle = noop_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		noop_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

TEST_CASE("Test noop read whole file", "[noop cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "noop";
	config.cache_block_size = TEST_FILE_SIZE;
	TestCacheFileSystemHelper helper(config);
	auto *noop_filesystem = helper.GetCacheFileSystem();

	auto handle = noop_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_FILE_SIZE;
	string content(bytes_to_read, '\0');
	noop_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
	                      start_offset);
	REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
}

int main(int argc, char **argv) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
	                        TEST_FILE_SIZE, /*location=*/0);
	file_handle->Sync();
	file_handle->Close();

	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveFile(TEST_FILENAME);
	return result;
}

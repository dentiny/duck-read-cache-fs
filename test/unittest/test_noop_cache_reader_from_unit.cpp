// Unit test for no-op cache filesystem (migrated from unit/).

#include "catch/catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "noop_cache_reader.hpp"
#include "scope_guard.hpp"
#include "test_constants.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

struct NoopCacheReaderFixture {
	string test_filename;
	NoopCacheReaderFixture() {
		test_filename = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
		auto local_filesystem = LocalFileSystem::CreateLocal();
		auto file_handle = local_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
		                        TEST_FILE_SIZE, /*location=*/0);
		file_handle->Sync();
		file_handle->Close();
	}
	~NoopCacheReaderFixture() {
		LocalFileSystem::CreateLocal()->RemoveFile(test_filename);
	}
};

} // namespace

TEST_CASE_METHOD(NoopCacheReaderFixture, "Test on noop cache filesystem", "[noop cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "noop";
	config.cache_block_size = TEST_FILE_SIZE;
	TestCacheFileSystemHelper helper(std::move(config));
	auto *noop_filesystem = helper.GetCacheFileSystem();

	// First uncached read.
	{
		auto handle = noop_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		noop_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second uncached read.
	{
		auto handle = noop_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		noop_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

TEST_CASE_METHOD(NoopCacheReaderFixture, "Test noop read whole file", "[noop cache filesystem test]") {
	TestCacheConfig config;
	config.cache_type = "noop";
	config.cache_block_size = TEST_FILE_SIZE;
	TestCacheFileSystemHelper helper(std::move(config));
	auto *noop_filesystem = helper.GetCacheFileSystem();

	auto handle = noop_filesystem->OpenFile(test_filename, FileOpenFlags::FILE_FLAGS_READ);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_FILE_SIZE;
	string content(bytes_to_read, '\0');
	noop_filesystem->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
	                      start_offset);
	REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
}

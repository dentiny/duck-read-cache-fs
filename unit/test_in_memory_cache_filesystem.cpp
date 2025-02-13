// Unit test for in-memory cache filesystem.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem_config.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "in_memory_cache_reader.hpp"
#include "scope_guard.hpp"

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

TEST_CASE("Test on in-memory cache filesystem", "[in-memory cache filesystem test]") {
	g_cache_block_size = TEST_FILE_SIZE;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	auto in_mem_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

	// First uncached read.
	{
		auto handle = in_mem_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}

	// Second cached read.
	{
		auto handle = in_mem_cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
		const uint64_t start_offset = 1;
		const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
		string content(bytes_to_read, '\0');
		in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
		                      start_offset);
		REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
	}
}

TEST_CASE("Test on concurrent access", "[in-memory cache filesystem test]") {
	g_cache_block_size = 5;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	auto in_mem_cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());

	auto handle = in_mem_cache_fs->OpenFile(TEST_FILENAME,
	                                        FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
	const uint64_t start_offset = 0;
	const uint64_t bytes_to_read = TEST_FILE_SIZE;

	// Spawn multiple threads to read through in-memory cache filesystem.
	constexpr idx_t THREAD_NUM = 200;
	vector<thread> reader_threads;
	reader_threads.reserve(THREAD_NUM);
	for (idx_t idx = 0; idx < THREAD_NUM; ++idx) {
		reader_threads.emplace_back([&]() {
			string content(bytes_to_read, '\0');
			in_mem_cache_fs->Read(*handle, const_cast<void *>(static_cast<const void *>(content.data())), bytes_to_read,
			                      start_offset);
			REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
		});
	}
	for (auto &cur_thd : reader_threads) {
		D_ASSERT(cur_thd.joinable());
		cur_thd.join();
	}
}

int main(int argc, char **argv) {
	// Set global cache type for testing.
	g_test_cache_type = IN_MEM_CACHE_TYPE;

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

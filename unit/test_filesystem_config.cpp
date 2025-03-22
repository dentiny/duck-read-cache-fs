#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "base_profile_collector.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_reader_manager.hpp"
#include "disk_cache_reader.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "in_memory_cache_reader.hpp"
#include "noop_cache_reader.hpp"
#include "temp_profile_collector.hpp"

using namespace duckdb; // NOLINT

namespace {
const auto TEST_FILENAME = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
} // namespace

TEST_CASE("Filesystem config test", "[filesystem config]") {
	REQUIRE(GetThreadCountForSubrequests(10) == 10);

	g_max_subrequest_count = 5;
	REQUIRE(GetThreadCountForSubrequests(10) == 5);
}

TEST_CASE("Filesystem cache config test", "[filesystem config]") {
	DuckDB db {};
	StandardBufferManager buffer_manager {*db.instance, "/tmp/cache_httpfs_fs_benchmark"};
	auto cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());
	auto client_context = make_shared_ptr<ClientContext>(db.instance);
	auto &cache_reader_manager = CacheReaderManager::Get();

	// Check noop cache reader.
	{
		client_context->config.set_variables["cache_httpfs_type"] = Value(*NOOP_CACHE_TYPE);
		ClientContextFileOpener file_opener {*client_context};
		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ, &file_opener);
		auto *cache_reader = cache_reader_manager.GetCacheReader();
		[[maybe_unused]] auto &noop_handle = cache_reader->Cast<NoopCacheReader>();
	}

	// Check in-memory cache reader.
	{
		client_context->config.set_variables["cache_httpfs_type"] = Value(*IN_MEM_CACHE_TYPE);
		ClientContextFileOpener file_opener {*client_context};
		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ, &file_opener);
		auto *cache_reader = cache_reader_manager.GetCacheReader();
		[[maybe_unused]] auto &noop_handle = cache_reader->Cast<InMemoryCacheReader>();
	}

	// Check on-disk cache reader.
	{
		client_context->config.set_variables["cache_httpfs_type"] = Value(*ON_DISK_CACHE_TYPE);
		ClientContextFileOpener file_opener {*client_context};
		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ, &file_opener);
		auto *cache_reader = cache_reader_manager.GetCacheReader();
		[[maybe_unused]] auto &noop_handle = cache_reader->Cast<DiskCacheReader>();
	}
}

TEST_CASE("Filesystem profile config test", "[filesystem config]") {
	DuckDB db {};
	StandardBufferManager buffer_manager {*db.instance, "/tmp/cache_httpfs_fs_benchmark"};
	auto cache_fs = make_uniq<CacheFileSystem>(LocalFileSystem::CreateLocal());
	auto client_context = make_shared_ptr<ClientContext>(db.instance);

	// Check noop profiler.
	{
		client_context->config.set_variables["cache_httpfs_profile_type"] = Value(*NOOP_PROFILE_TYPE);
		ClientContextFileOpener file_opener {*client_context};
		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ, &file_opener);
		auto *profiler = cache_fs->GetProfileCollector();
		[[maybe_unused]] auto &noop_profiler = profiler->Cast<NoopProfileCollector>();
	}

	// Check temp cache reader.
	{
		client_context->config.set_variables["cache_httpfs_profile_type"] = Value(*TEMP_PROFILE_TYPE);
		ClientContextFileOpener file_opener {*client_context};
		cache_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ, &file_opener);
		auto *profiler = cache_fs->GetProfileCollector();
		[[maybe_unused]] auto &temp_profiler = profiler->Cast<TempProfileCollector>();
	}
}

int main(int argc, char **argv) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	int result = Catch::Session().run(argc, argv);
	local_filesystem->RemoveFile(TEST_FILENAME);
	return result;
}

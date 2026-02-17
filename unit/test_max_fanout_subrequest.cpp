// Unit test for cache_httpfs_max_fanout_subrequest.
//
// This verifies that a single read request does not fan out more concurrent
// subrequests than the configured limit.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "cache_filesystem.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"
#include "scoped_directory.hpp"

#include <atomic>
#include <chrono>
#include <thread>

using namespace duckdb; // NOLINT

namespace {

constexpr idx_t TEST_FILE_SIZE = 16 * 1024;
constexpr idx_t BLOCK_SIZE = 64;
constexpr idx_t MAX_FANOUT = 1;
constexpr std::chrono::milliseconds PER_READ_DELAY {10};

class SlowTrackingFileSystem : public LocalFileSystem {
public:
	explicit SlowTrackingFileSystem(std::chrono::milliseconds per_read_delay_p)
	    : local_filesystem(LocalFileSystem::CreateLocal()), per_read_delay(per_read_delay_p) {
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override {
		return local_filesystem->OpenFile(path, flags, opener);
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		const int current_count = current_read_count.fetch_add(1) + 1;
		UpdateMaxConcurrentReadCount(current_count);

		// Add delay to increase overlap between reads, so this test can catch
		// over-parallelized fanout if the cap is not respected.
		std::this_thread::sleep_for(per_read_delay);
		local_filesystem->Read(handle, buffer, nr_bytes, location);

		current_read_count.fetch_sub(1);
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return local_filesystem->GetFileSize(handle);
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return local_filesystem->GetLastModifiedTime(handle);
	}

	void Seek(FileHandle &handle, idx_t location) override {
		local_filesystem->Seek(handle, location);
	}

	idx_t SeekPosition(FileHandle &handle) override {
		return local_filesystem->SeekPosition(handle);
	}

	std::string GetName() const override {
		return "slow_tracking_filesystem";
	}

	int GetMaxConcurrentReadCount() const {
		return max_concurrent_read_count.load();
	}

private:
	void UpdateMaxConcurrentReadCount(int current_count) {
		int max_count = max_concurrent_read_count.load();
		while (current_count > max_count &&
		       !max_concurrent_read_count.compare_exchange_weak(max_count, current_count)) {
		}
	}

private:
	unique_ptr<FileSystem> local_filesystem;
	std::chrono::milliseconds per_read_delay;
	std::atomic<int> current_read_count {0};
	std::atomic<int> max_concurrent_read_count {0};
};

struct TestFsHelper {
	DuckDB db;
	shared_ptr<CacheHttpfsInstanceState> instance_state;
	unique_ptr<CacheFileSystem> cache_fs;
	SlowTrackingFileSystem *slow_fs = nullptr;

	TestFsHelper(unique_ptr<SlowTrackingFileSystem> slow_fs_p, const string &cache_type,
	             const string &cache_directory) {
		instance_state = make_shared_ptr<CacheHttpfsInstanceState>();

		auto &config = instance_state->config;
		config.cache_type = cache_type;
		config.cache_block_size = BLOCK_SIZE;
		config.max_subrequest_count = MAX_FANOUT;
		config.enable_cache_validation = false;
		config.enable_disk_reader_mem_cache = false;
		config.on_disk_cache_directories = {cache_directory};

		auto local_filesystem = LocalFileSystem::CreateLocal();
		local_filesystem->CreateDirectory(cache_directory);

		SetInstanceState(*db.instance.get(), instance_state);
		instance_state->cache_reader_manager.SetCacheReader(config, instance_state);
		slow_fs = slow_fs_p.get();
		cache_fs = make_uniq<CacheFileSystem>(std::move(slow_fs_p), instance_state);
	}
};

string BuildTestFileContent() {
	string content(TEST_FILE_SIZE, '\0');
	for (idx_t idx = 0; idx < content.size(); ++idx) {
		content[idx] = NumericCast<char>('a' + (idx % 26));
	}
	return content;
}

int RunReadAndGetMaxConcurrency(const string &cache_type) {
	const string test_directory =
	    StringUtil::Format("/tmp/cache_httpfs_fanout_%s", UUID::ToString(UUID::GenerateRandomUUID()));
	ScopedDirectory scoped_directory(test_directory);
	const string source_path = StringUtil::Format("%s/source.data", scoped_directory.GetPath());
	const string cache_dir = StringUtil::Format("%s/cache", scoped_directory.GetPath());

	auto local_filesystem = LocalFileSystem::CreateLocal();

	const string source_content = BuildTestFileContent();
	{
		auto file_handle = local_filesystem->OpenFile(source_path, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                               FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<char *>(source_content.data()), source_content.size(), 0);
		file_handle->Sync();
		file_handle->Close();
	}

	auto slow_fs = make_uniq<SlowTrackingFileSystem>(PER_READ_DELAY);
	TestFsHelper helper(std::move(slow_fs), cache_type, cache_dir);
	auto *cache_fs = helper.cache_fs.get();

	auto file_handle = cache_fs->OpenFile(source_path, FileOpenFlags::FILE_FLAGS_READ);
	string read_output(source_content.size(), '\0');
	cache_fs->Read(*file_handle, const_cast<char *>(read_output.data()), read_output.size(), 0);
	REQUIRE(read_output == source_content);

	return helper.slow_fs->GetMaxConcurrentReadCount();
}

} // namespace

TEST_CASE("Test max fanout subrequest on in-memory cache reader", "[max fanout subrequest]") {
	const int max_concurrency = RunReadAndGetMaxConcurrency("in_mem");
	REQUIRE(max_concurrency <= MAX_FANOUT);
}

TEST_CASE("Test max fanout subrequest on on-disk cache reader", "[max fanout subrequest]") {
	const int max_concurrency = RunReadAndGetMaxConcurrency("on_disk");
	REQUIRE(max_concurrency <= MAX_FANOUT);
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}

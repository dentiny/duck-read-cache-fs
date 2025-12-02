// Benchmark setup: start from a random offset, and read 10 bytes every time (which is much less than block size).

#include "cache_filesystem.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "rand_utils.hpp"
#include "s3fs.hpp"
#include "time_utils.hpp"

#include <csignal>
#include <iostream>

namespace {
const std::string BENCHMARK_DISK_CACHE_DIRECTORY = "/tmp/benchmark_cache";
constexpr uint64_t BYTES_TO_READ = 10;
constexpr uint64_t BENCHMARK_RUNS = 25;
} // namespace

namespace duckdb {

namespace {

struct BenchmarkSetup {
	std::string cache_type;
	std::string profile_type;
	std::string disk_cache_directory;
	uint64_t block_size = DEFAULT_CACHE_BLOCK_SIZE;
};

void SetConfig(case_insensitive_map_t<Value> &setting, char *env_key, char *secret_key) {
	const char *env_val = getenv(env_key);
	if (env_val == nullptr) {
		return;
	}
	setting[secret_key] = Value(env_val);
}

void SetOpenerConfig(const shared_ptr<ClientContext> &ctx, const BenchmarkSetup &benchmark_setup) {
	auto &set_vars = ctx->config.set_variables;
	SetConfig(set_vars, "AWS_DEFAULT_REGION", "s3_region");
	SetConfig(set_vars, "AWS_ACCESS_KEY_ID", "s3_access_key_id");
	SetConfig(set_vars, "AWS_SECRET_ACCESS_KEY", "s3_secret_access_key");
	set_vars["cache_httpfs_profile_type"] = Value(benchmark_setup.profile_type);
	set_vars["cache_httpfs_type"] = Value(benchmark_setup.cache_type);
	set_vars["cache_httpfs_cache_directory"] = Value(benchmark_setup.disk_cache_directory);
	set_vars["cache_httpfs_cache_block_size"] = Value::UBIGINT(benchmark_setup.block_size);
}

void TestSequentialRead(const BenchmarkSetup &benchmark_setup) {
	DuckDB db {};
	StandardBufferManager buffer_manager {*db.instance, "/tmp/cache_httpfs_fs_benchmark"};
	auto s3fs = make_uniq<S3FileSystem>(buffer_manager);
	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	auto cache_fs = make_uniq<CacheFileSystem>(std::move(s3fs), std::move(instance_state));
	auto client_context = make_shared_ptr<ClientContext>(db.instance);

	SetOpenerConfig(client_context, benchmark_setup);
	ClientContextFileOpener file_opener {*client_context};
	client_context->transaction.BeginTransaction();

	auto file_handle =
	    cache_fs->OpenFile("s3://duckdb-cache-fs/lineitem.parquet", FileOpenFlags::FILE_FLAGS_READ, &file_opener);
	const uint64_t file_size = cache_fs->GetFileSize(*file_handle);
	std::string buffer(BYTES_TO_READ, '\0');

	const auto start = GetSteadyNowMilliSecSinceEpoch();

	for (uint16_t ii = 0; ii < BENCHMARK_RUNS; ++ii) {
		const uint64_t start_offset = GetRandomValueInRange<uint64_t>(0, file_size);
		const uint64_t cur_bytes_to_read = MinValue<uint64_t>(BYTES_TO_READ, file_size - start_offset);
		cache_fs->Read(*file_handle, const_cast<char *>(buffer.data()), /*nr_bytes=*/cur_bytes_to_read,
		               /*location=*/start_offset);
	}

	const auto end = GetSteadyNowMilliSecSinceEpoch();
	const auto duration_millisec = end - start;
	std::cout << BENCHMARK_RUNS << " runs of random read of " << BYTES_TO_READ << " bytes takes " << duration_millisec
	          << " milliseconds" << std::endl;
}

} // namespace

} // namespace duckdb

int main(int argc, char **argv) {
	// Ignore SIGPIPE, reference: https://blog.erratasec.com/2018/10/tcpip-sockets-and-sigpipe.html
	std::signal(SIGPIPE, SIG_IGN);

	// Warm up system resource (i.e. httpfs metadata cache, TCP congestion window, etc).
	std::cout << "Starts to warmup read" << std::endl;
	duckdb::FileSystem::CreateLocal()->RemoveDirectory(BENCHMARK_DISK_CACHE_DIRECTORY);
	duckdb::BenchmarkSetup benchmark_setup;
	benchmark_setup.cache_type = *duckdb::NOOP_CACHE_TYPE;
	benchmark_setup.profile_type = *duckdb::NOOP_PROFILE_TYPE;
	duckdb::TestSequentialRead(benchmark_setup);

	// Benchmark httpfs (with no cache reader).
	std::cout << "Starts with httpfs read with no cache" << std::endl;
	duckdb::FileSystem::CreateLocal()->RemoveDirectory(BENCHMARK_DISK_CACHE_DIRECTORY);
	benchmark_setup.cache_type = *duckdb::NOOP_CACHE_TYPE;
	benchmark_setup.profile_type = *duckdb::TEMP_PROFILE_TYPE;
	benchmark_setup.disk_cache_directory = BENCHMARK_DISK_CACHE_DIRECTORY;
	duckdb::TestSequentialRead(benchmark_setup);

	// Benchmark on-disk cache reader.
	std::cout << "Starts on-disk cache read with no existing cache" << std::endl;
	duckdb::FileSystem::CreateLocal()->RemoveDirectory(BENCHMARK_DISK_CACHE_DIRECTORY);
	benchmark_setup.cache_type = *duckdb::ON_DISK_CACHE_TYPE;
	benchmark_setup.profile_type = *duckdb::TEMP_PROFILE_TYPE;
	benchmark_setup.disk_cache_directory = BENCHMARK_DISK_CACHE_DIRECTORY;
	duckdb::TestSequentialRead(benchmark_setup);

	// Cleanup on-disk cache after benchmark.
	duckdb::FileSystem::CreateLocal()->RemoveDirectory(BENCHMARK_DISK_CACHE_DIRECTORY);

	return 0;
}

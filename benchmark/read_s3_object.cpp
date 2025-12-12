// This file serves as a benchmark to read a whole S3 objects; it only tests
// uncached read.

#include "cache_filesystem.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "s3fs.hpp"

#include <array>
#include <csignal>
#include <iostream>

namespace duckdb {

namespace {

// Set configuration in client context from env variables.
void SetConfig(case_insensitive_map_t<Value> &setting, char *env_key, char *secret_key) {
	const char *env_val = getenv(env_key);
	if (env_val == nullptr) {
		return;
	}
	setting[secret_key] = Value(env_val);
}

// Baseline benchmark, which reads the whole file with no parallelism and
// caching.
void BaseLineRead() {
	DuckDB db {};
	StandardBufferManager buffer_manager {*db.instance, "/tmp/cache_httpfs_fs_benchmark"};
	auto s3fs = make_uniq<S3FileSystem>(buffer_manager);

	auto client_context = make_shared_ptr<ClientContext>(db.instance);
	auto &set_vars = client_context->config.set_variables;
	SetConfig(set_vars, "AWS_DEFAULT_REGION", "s3_region");
	SetConfig(set_vars, "AWS_ACCESS_KEY_ID", "s3_access_key_id");
	SetConfig(set_vars, "AWS_SECRET_ACCESS_KEY", "s3_secret_access_key");
	SetConfig(set_vars, "DUCKDB_S3_ENDPOINT", "s3_endpoint");

	ClientContextFileOpener file_opener {*client_context};
	client_context->transaction.BeginTransaction();
	auto file_handle = s3fs->OpenFile("s3://s3-bucket-user-2skzy8zuigonczyfiofztl0zbug--use1-az6--x-s3/"
	                                  "large-csv.csv",
	                                  FileOpenFlags::FILE_FLAGS_READ, &file_opener);
	const uint64_t file_size = s3fs->GetFileSize(*file_handle);
	std::string content(file_size, '\0');

	const auto now = std::chrono::steady_clock::now();
	s3fs->Read(*file_handle, const_cast<char *>(content.data()), file_size,
	           /*location=*/0);
	const auto end = std::chrono::steady_clock::now();
	const auto duration_sec = std::chrono::duration_cast<std::chrono::duration<double>>(end - now).count();
	std::cout << "Baseline S3 filesystem reads " << file_size << " bytes takes " << duration_sec << " seconds"
	          << std::endl;
}

void ReadUncachedWholeFile(uint64_t block_size) {
	DuckDB db {};
	StandardBufferManager buffer_manager {*db.instance, "/tmp/cache_httpfs_fs_benchmark"};
	auto s3fs = make_uniq<S3FileSystem>(buffer_manager);

	// Use default cache directory
	vector<string> cache_directories = {*DEFAULT_ON_DISK_CACHE_DIRECTORY};
	for (const auto &cur_cache_dir : cache_directories) {
		LocalFileSystem::CreateLocal()->RemoveDirectory(cur_cache_dir);
	}

	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	auto disk_cache_fs = make_uniq<CacheFileSystem>(std::move(s3fs), std::move(instance_state));

	auto client_context = make_shared_ptr<ClientContext>(db.instance);
	auto &set_vars = client_context->config.set_variables;
	SetConfig(set_vars, "AWS_DEFAULT_REGION", "s3_region");
	SetConfig(set_vars, "AWS_ACCESS_KEY_ID", "s3_access_key_id");
	SetConfig(set_vars, "AWS_SECRET_ACCESS_KEY", "s3_secret_access_key");
	SetConfig(set_vars, "DUCKDB_S3_ENDPOINT", "s3_endpoint");
	// Set the block size via configuration
	set_vars["cache_httpfs_cache_block_size"] = Value::UBIGINT(block_size);
	ClientContextFileOpener file_opener {*client_context};
	client_context->transaction.BeginTransaction();

	auto file_handle = disk_cache_fs->OpenFile("s3://s3-bucket-user-2skzy8zuigonczyfiofztl0zbug--use1-az6--x-s3/"
	                                           "large-csv.csv",
	                                           FileOpenFlags::FILE_FLAGS_READ, &file_opener);
	const uint64_t file_size = disk_cache_fs->GetFileSize(*file_handle);
	std::string content(file_size, '\0');

	auto read_whole_file = [&]() {
		const auto now = std::chrono::steady_clock::now();
		disk_cache_fs->Read(*file_handle, const_cast<char *>(content.data()), file_size, /*location=*/0);
		const auto end = std::chrono::steady_clock::now();
		const auto duration_sec = std::chrono::duration_cast<std::chrono::duration<double>>(end - now).count();
		std::cout << "Cached http filesystem reads " << file_size << " bytes with block size " << block_size
		          << " takes " << duration_sec << " seconds" << std::endl;
	};

	// Uncached but parallel read.
	read_whole_file();
	// Cached and parallel read.
	read_whole_file();
}

} // namespace

} // namespace duckdb

int main(int argc, char **argv) {
	// Ignore SIGPIPE, reference: https://blog.erratasec.com/2018/10/tcpip-sockets-and-sigpipe.html
	std::signal(SIGPIPE, SIG_IGN);

	constexpr std::array<uint64_t, 10> BLOCK_SIZE_ARR {
	    64ULL * 1024,        // 64KiB
	    256ULL * 1024,       // 256KiB
	    1ULL * 1024 * 1024,  // 1MiB
	    4ULL * 1024 * 1024,  // 4MiB
	    16ULL * 1024 * 1024, // 16MiB
	};

	duckdb::BaseLineRead();
	for (uint64_t cur_block_size : BLOCK_SIZE_ARR) {
		duckdb::ReadUncachedWholeFile(cur_block_size);
	}
	return 0;
}

#include "disk_cache_reader.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "s3fs.hpp"
#include "scope_guard.hpp"

#include <csignal>
#include <array>
#include <iostream>

namespace duckdb {

namespace {

void SetConfig(case_insensitive_map_t<Value> &setting, char *env_key, char *secret_key) {
	const char *env_val = getenv(env_key);
	if (env_val == nullptr) {
		return;
	}
	setting[secret_key] = Value(env_val);
}

void SetSecretConfig(shared_ptr<ClientContext> ctx) {
   auto &set_vars = ctx->config.set_variables;
	SetConfig(set_vars, "AWS_DEFAULT_REGION", "s3_region");
	SetConfig(set_vars, "AWS_ACCESS_KEY_ID", "s3_access_key_id");
	SetConfig(set_vars, "AWS_SECRET_ACCESS_KEY", "s3_secret_access_key");
}

void TestSequentialRead() {	
    DuckDB db {};
    StandardBufferManager buffer_manager {*db.instance, "/tmp/cached_http_fs_benchmark"};
    auto s3fs = make_uniq<S3FileSystem>(buffer_manager);

	std::cout << g_on_disk_cache_directory << std::endl;
    FileSystem::CreateLocal()->RemoveDirectory(g_on_disk_cache_directory);
    auto cache_fs = make_uniq<CacheFileSystem>(std::move(s3fs));

    auto client_context = make_shared_ptr<ClientContext>(db.instance);
    SetSecretConfig(client_context);
    ClientContextFileOpener file_opener {*client_context};
    client_context->transaction.BeginTransaction();
    auto file_handle = cache_fs->OpenFile("s3://duckdb-cache-fs/"
                                    "lineitem.parquet",
                                    FileOpenFlags::FILE_FLAGS_READ, &file_opener);	
    const uint64_t file_size = cache_fs->GetFileSize(*file_handle);
	
    const size_t chunk_size = 32_MiB;
    std::string buffer(chunk_size, '\0');

    auto read_sequential = [&]() {
        uint64_t bytes_read = 0;
        const auto now = std::chrono::steady_clock::now();
        
        while (bytes_read < file_size) {
            size_t current_chunk = static_cast<size_t>(std::min(static_cast<uint64_t>(chunk_size), file_size - bytes_read));
            cache_fs->Read(*file_handle, const_cast<char *>(buffer.data()), 
                               current_chunk, bytes_read);
            bytes_read += current_chunk;
        }

        const auto end = std::chrono::steady_clock::now();
        const auto duration_sec = std::chrono::duration_cast<std::chrono::duration<double>>(end - now).count();
        std::cout << "Sequential read of " << file_size << " bytes in " 
                  << chunk_size << "-byte chunks takes " << duration_sec 
                  << " seconds" << std::endl;
    };

	if(g_test_cache_type == duckdb::NOOP_CACHE_TYPE) {
		read_sequential();
		return;
	} else if (g_test_cache_type == duckdb::ON_DISK_CACHE_TYPE) {
		std::cout << "--------------------- Performing Uncached Read---------------------" << std::endl;
		read_sequential();
		std::cout << "--------------------- Performing cached  Read ---------------------" << std::endl;
		read_sequential();
	}
}

} // namespace

} // namespace duckdb

int main(int argc, char **argv) {
	std::signal(SIGPIPE, SIG_IGN);
	duckdb::g_test_cache_type = duckdb::NOOP_CACHE_TYPE;
	duckdb::g_on_disk_cache_directory = "/tmp/benchmark_cache";
	// Warm Up Read();
	// duckdb::TestSequentialRead();
	
	// Benchmark Noop cache Read();
	duckdb::TestSequentialRead();

	// Benchmark On Disk cache Read();
	// It will perform uncached read first, then cached read.
	duckdb::g_test_cache_type = duckdb::ON_DISK_CACHE_TYPE;
	duckdb::TestSequentialRead();
	return 0;
}

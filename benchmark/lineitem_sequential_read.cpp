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


void BaseLineRead() {
	DuckDB db {};
	StandardBufferManager buffer_manager {*db.instance, "/tmp/cached_http_fs_benchmark"};
	auto s3fs = make_uniq<S3FileSystem>(buffer_manager);

	auto client_context = make_shared_ptr<ClientContext>(db.instance);
    SetSecretConfig(client_context);
	
	ClientContextFileOpener file_opener {*client_context};
	client_context->transaction.BeginTransaction();
	auto file_handle = s3fs->OpenFile("s3://duckdb-cache-fs/"
	                                  "lineitem.parquet",
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

void ReadUncachedWholeFile() {
	g_cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
	g_cache_type = DEFAULT_ON_DISK_CACHE_DIRECTORY;
	SCOPE_EXIT {
		ResetGlobalConfig();
	};

	DuckDB db {};
	StandardBufferManager buffer_manager {*db.instance, "/tmp/cached_http_fs_benchmark"};
	auto s3fs = make_uniq<S3FileSystem>(buffer_manager);

	FileSystem::CreateLocal()->RemoveDirectory(g_on_disk_cache_directory);
	auto disk_cache_fs = make_uniq<CacheFileSystem>(std::move(s3fs));

	auto client_context = make_shared_ptr<ClientContext>(db.instance);
	SetSecretConfig(client_context);
	ClientContextFileOpener file_opener {*client_context};
	client_context->transaction.BeginTransaction();
	auto file_handle = disk_cache_fs->OpenFile("s3://duckdb-cache-fs/"
	                                  "lineitem.parquet",
	                                  FileOpenFlags::FILE_FLAGS_READ, &file_opener);

	const uint64_t file_size = disk_cache_fs->GetFileSize(*file_handle);
	std::string content(file_size, '\0');

	auto read_whole_file = [&]() {
		const auto now = std::chrono::steady_clock::now();
		disk_cache_fs->Read(*file_handle, const_cast<char *>(content.data()), file_size, /*location=*/0);
		const auto end = std::chrono::steady_clock::now();
		const auto duration_sec = std::chrono::duration_cast<std::chrono::duration<double>>(end - now).count();
		std::cout << "Cached http filesystem reads " << file_size << " takes " << duration_sec << " seconds" << std::endl;
	};

	// Uncached but parallel read.
	read_whole_file();
	// Cached and parallel read.
	read_whole_file();
}

void ReadSequential16Bytes() {
    g_cache_block_size = DEFAULT_CACHE_BLOCK_SIZE;
    g_cache_type = DEFAULT_ON_DISK_CACHE_DIRECTORY;
    SCOPE_EXIT {
        ResetGlobalConfig();
    };

    DuckDB db {};
    StandardBufferManager buffer_manager {*db.instance, "/tmp/cached_http_fs_benchmark"};
    auto s3fs = make_uniq<S3FileSystem>(buffer_manager);

    FileSystem::CreateLocal()->RemoveDirectory(g_on_disk_cache_directory);
    auto disk_cache_fs = make_uniq<CacheFileSystem>(std::move(s3fs));

    auto client_context = make_shared_ptr<ClientContext>(db.instance);
    SetSecretConfig(client_context);
    ClientContextFileOpener file_opener {*client_context};
    client_context->transaction.BeginTransaction();
    auto file_handle = disk_cache_fs->OpenFile("s3://duckdb-cache-fs/"
                                    "lineitem.parquet",
                                    FileOpenFlags::FILE_FLAGS_READ, &file_opener);

    const uint64_t file_size = disk_cache_fs->GetFileSize(*file_handle);
    const size_t chunk_size = 16;
    std::string buffer(chunk_size, '\0');

    auto read_sequential = [&]() {
        uint64_t bytes_read = 0;
        const auto now = std::chrono::steady_clock::now();
        
        while (bytes_read < file_size) {
            size_t current_chunk = static_cast<size_t>(std::min(static_cast<uint64_t>(chunk_size), file_size - bytes_read));
            disk_cache_fs->Read(*file_handle, const_cast<char *>(buffer.data()), 
                               current_chunk, bytes_read);
            bytes_read += current_chunk;
        }

        const auto end = std::chrono::steady_clock::now();
        const auto duration_sec = std::chrono::duration_cast<std::chrono::duration<double>>(end - now).count();
        std::cout << "Sequential read of " << file_size << " bytes in " 
                  << chunk_size << "-byte chunks takes " << duration_sec 
                  << " seconds" << std::endl;
    };

    // Uncached sequential read
    std::cout << "Performing uncached sequential read..." << std::endl;
    read_sequential();
    
    // Cached sequential read
    std::cout << "Performing cached sequential read..." << std::endl;
    read_sequential();
}



} // namespace

} // namespace duckdb

int main(int argc, char **argv) {
	std::signal(SIGPIPE, SIG_IGN);
	// duckdb::BaseLineRead();
	// duckdb::ReadUncachedWholeFile();
	duckdb::ReadSequential16Bytes();
	return 0;
}

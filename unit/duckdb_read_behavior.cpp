#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "s3fs.hpp"

#include <iostream>

using namespace duckdb;

void ReadFromLocalFileSystem() {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile("/tmp/cache_file", FileOpenFlags::FILE_FLAGS_READ);
	char buffer = 'a';
	file_handle->Read(&buffer, /*nr_bytes=*/1, /*location=*/0);
	std::cerr << "Offset = " << file_handle->SeekPosition() << std::endl; // offset = 0
}

// Set configuration in client context from env variables.
void SetConfig(case_insensitive_map_t<Value> &setting, char *env_key, char *secret_key) {
	const char *env_val = getenv(env_key);
	if (env_val == nullptr) {
		return;
	}
	setting[secret_key] = Value(env_val);
}

void ReadDFromS3() {
	DuckDB db {};
	StandardBufferManager buffer_manager {*db.instance, "/tmp/cache_httpfs_fs_benchmark"};
	auto s3fs = make_uniq<S3FileSystem>(buffer_manager);

	auto client_context = make_shared_ptr<ClientContext>(db.instance);
	auto &set_vars = client_context->config.set_variables;
	SetConfig(set_vars, "AWS_DEFAULT_REGION", "s3_region");
	SetConfig(set_vars, "AWS_ACCESS_KEY_ID", "s3_access_key_id");
	SetConfig(set_vars, "AWS_SECRET_ACCESS_KEY", "s3_secret_access_key");

	ClientContextFileOpener file_opener {*client_context};
	client_context->transaction.BeginTransaction();
	auto file_handle =
	    s3fs->OpenFile("s3://duckdb-cache-fs/lineitem.parquet", FileOpenFlags::FILE_FLAGS_READ, &file_opener);
	char buffer = 'a';
	file_handle->Read(&buffer, /*nr_bytes=*/1, /*location=*/0);
	std::cerr << "Offset = " << file_handle->SeekPosition() << std::endl; // offset = 1
}

int main() {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	auto file_handle = local_filesystem->OpenFile("/tmp/cache_file", FileOpenFlags::FILE_FLAGS_WRITE |
	                                                                     FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	const string content = "helloworld";
	local_filesystem->Write(*file_handle, const_cast<char *>(content.data()),
	                        /*nr_bytes=*/content.length(),
	                        /*location=*/0);
	file_handle->Sync();
	file_handle->Close();

	ReadFromLocalFileSystem();
	ReadDFromS3();

	return 0;
}

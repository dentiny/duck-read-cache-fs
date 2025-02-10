#include "base_cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"

namespace duckdb {

CacheFileSystemHandle::CacheFileSystemHandle(unique_ptr<FileHandle> internal_file_handle_p, CacheFileSystem &fs)
    : FileHandle(fs, internal_file_handle_p->GetPath(), internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {
}

bool CacheFileSystem::CanHandleFile(const string &fpath) {
	if (internal_filesystem->CanHandleFile(fpath)) {
		return true;
	}

	// Special handle cases where local filesystem is the internal filesystem.
	//
	// duckdb's implementation is `LocalFileSystem::CanHandle` always returns
	// false, to enable cached local filesystem (i.e. make in-memory cache for
	// disk access), we inherit the virtual filesystem's assumption that local
	// filesystem is the fallback type, which could potentially handles
	// everything.
	//
	// If it doesn't work with local filesystem, an error is returned anyway.
	if (internal_filesystem->GetName() == "LocalFileSystem") {
		return true;
	}

	return false;
}

bool CacheFileSystem::IsManuallySet() {
	// As documented at [CanHandleFile], local filesystem serves as the fallback
	// filesystem for all given paths, return false in certain case so virtual
	// filesystem could pick the most suitable one if exists.
	if (internal_filesystem->GetName() == "LocalFileSystem") {
		return false;
	}

	return true;
}

void CacheFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ReadImpl(handle, buffer, nr_bytes, location);
}
int64_t CacheFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	const int64_t bytes_read = ReadImpl(handle, buffer, nr_bytes, handle.SeekPosition());
	handle.Seek(handle.SeekPosition() + bytes_read);
	return bytes_read;
}
int64_t CacheFileSystem::ReadImpl(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	const auto file_size = handle.GetFileSize();

	// No more bytes to read.
	if (location == file_size) {
		return 0;
	}

	const int64_t bytes_to_read = MinValue<int64_t>(nr_bytes, file_size - location);
	ReadAndCache(handle, static_cast<char *>(buffer), location, bytes_to_read, file_size);

	return bytes_to_read;
}

} // namespace duckdb

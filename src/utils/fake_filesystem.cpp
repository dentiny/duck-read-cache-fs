#include "duckdb/common/string_util.hpp"
#include "fake_filesystem.hpp"

namespace duckdb {

namespace {
const std::string FAKE_FILESYSTEM_PREFIX = "/tmp/cache_httpfs_fake_filesystem";
} // namespace

CacheHttpfsFakeFsHandle::CacheHttpfsFakeFsHandle(string path, unique_ptr<FileHandle> internal_file_handle_p,
                                                 CacheHttpfsFakeFileSystem &fs)
    : FileHandle(fs, std::move(path), internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {
}
CacheHttpfsFakeFileSystem::CacheHttpfsFakeFileSystem() : local_filesystem(LocalFileSystem::CreateLocal()) {
	local_filesystem->CreateDirectory(FAKE_FILESYSTEM_PREFIX);
}
bool CacheHttpfsFakeFileSystem::CanHandleFile(const string &path) {
	return StringUtil::StartsWith(path, FAKE_FILESYSTEM_PREFIX);
}

unique_ptr<FileHandle> CacheHttpfsFakeFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                           optional_ptr<FileOpener> opener) {
	auto file_handle = local_filesystem->OpenFile(path, flags, opener);
	return make_uniq<CacheHttpfsFakeFsHandle>(path, std::move(file_handle), *this);
}
void CacheHttpfsFakeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	local_filesystem->Read(*local_filesystem_handle, buffer, nr_bytes, location);
}
int64_t CacheHttpfsFakeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->Read(*local_filesystem_handle, buffer, nr_bytes);
}

void CacheHttpfsFakeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	local_filesystem->Write(*local_filesystem_handle, buffer, nr_bytes, location);
}
int64_t CacheHttpfsFakeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->Write(*local_filesystem_handle, buffer, nr_bytes);
}
int64_t CacheHttpfsFakeFileSystem::GetFileSize(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->GetFileSize(*local_filesystem_handle);
}
void CacheHttpfsFakeFileSystem::FileSync(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	local_filesystem->FileSync(*local_filesystem_handle);
}

void CacheHttpfsFakeFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	local_filesystem->Seek(*local_filesystem_handle, location);
}
idx_t CacheHttpfsFakeFileSystem::SeekPosition(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->SeekPosition(*local_filesystem_handle);
}

} // namespace duckdb

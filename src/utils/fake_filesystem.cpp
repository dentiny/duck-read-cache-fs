#include "fake_filesystem.hpp"

#include "duckdb/common/string_util.hpp"
#include "filesystem_utils.hpp"
#include "no_destructor.hpp"

namespace duckdb {

CacheHttpfsFakeFsHandle::CacheHttpfsFakeFsHandle(string path, unique_ptr<FileHandle> internal_file_handle_p,
                                                 CacheHttpfsFakeFileSystem &fs)
    : FileHandle(fs, std::move(path), internal_file_handle_p->GetFlags()),
      internal_file_handle(std::move(internal_file_handle_p)) {
}
CacheHttpfsFakeFileSystem::CacheHttpfsFakeFileSystem() : local_filesystem(LocalFileSystem::CreateLocal()) {
	local_filesystem->CreateDirectory(GetFakeOnDiskCacheDirectory());
}
bool CacheHttpfsFakeFileSystem::CanHandleFile(const string &path) {
	return StringUtil::StartsWith(path, GetFakeOnDiskCacheDirectory());
}

unique_ptr<FileHandle> CacheHttpfsFakeFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                           optional_ptr<FileOpener> opener) {
	auto file_handle = local_filesystem->OpenFile(path, flags, opener);
	auto handle = make_uniq<CacheHttpfsFakeFsHandle>(path, std::move(file_handle), *this);
	return std::move(handle);
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
bool CacheHttpfsFakeFileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->Trim(*local_filesystem_handle, offset_bytes, length_bytes);
}
timestamp_t CacheHttpfsFakeFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->GetLastModifiedTime(*local_filesystem_handle);
}
FileMetadata CacheHttpfsFakeFileSystem::Stats(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->Stats(*local_filesystem_handle);
}
string CacheHttpfsFakeFileSystem::GetVersionTag(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->GetVersionTag(*local_filesystem_handle);
}
FileType CacheHttpfsFakeFileSystem::GetFileType(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->GetFileType(*local_filesystem_handle);
}
void CacheHttpfsFakeFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	local_filesystem->Truncate(*local_filesystem_handle, new_size);
}
bool CacheHttpfsFakeFileSystem::OnDiskFile(FileHandle &handle) {
	auto &local_filesystem_handle = handle.Cast<CacheHttpfsFakeFsHandle>().internal_file_handle;
	return local_filesystem->OnDiskFile(*local_filesystem_handle);
}

} // namespace duckdb

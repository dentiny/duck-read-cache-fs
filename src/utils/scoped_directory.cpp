#include "scoped_directory.hpp"

#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

ScopedDirectory::ScopedDirectory(string directory_path_p) : directory_path(std::move(directory_path_p)) {
	auto local_filesystem = LocalFileSystem::CreateLocal();
	if (local_filesystem->DirectoryExists(directory_path)) {
		return;
	}
	local_filesystem->CreateDirectory(directory_path);
}

ScopedDirectory::~ScopedDirectory() {
	if (directory_path.empty()) {
		return;
	}
	auto local_filesystem = LocalFileSystem::CreateLocal();
	if (local_filesystem->DirectoryExists(directory_path)) {
		local_filesystem->RemoveDirectory(directory_path);
	}
}

} // namespace duckdb

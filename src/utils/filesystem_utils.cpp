#include "filesystem_utils.hpp"

#include <algorithm>
#include <ctime>

#include <sys/statvfs.h>

#include "cache_filesystem_config.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

void EvictStaleCacheFiles(FileSystem &local_filesystem, const string &cache_directory) {
	const time_t now = std::time(nullptr);
	local_filesystem.ListFiles(
	    cache_directory, [&local_filesystem, &cache_directory, now](const string &fname, bool /*unused*/) {
		    // Multiple threads could attempt to access and delete stale files,
		    // tolerate non-existent file.
		    const string full_name = StringUtil::Format("%s/%s", cache_directory, fname);
		    auto file_handle = local_filesystem.OpenFile(full_name, FileOpenFlags::FILE_FLAGS_READ |
		                                                                FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		    if (file_handle == nullptr) {
			    return;
		    }

		    const time_t last_mod_time = local_filesystem.GetLastModifiedTime(*file_handle);
		    const double diff = std::difftime(/*time_end=*/now, /*time_beg=*/last_mod_time);
		    if (static_cast<uint64_t>(diff) >= CACHE_FILE_STALENESS_SECOND) {
			    if (std::remove(full_name.data()) < -1 && errno != EEXIST) {
				    throw IOException("Fails to delete stale cache file %s", full_name);
			    }
		    }
	    });
}

int GetFileCountUnder(const std::string &folder) {
	int file_count = 0;
	LocalFileSystem::CreateLocal()->ListFiles(
	    folder, [&file_count](const string & /*unused*/, bool /*unused*/) { ++file_count; });
	return file_count;
}

vector<std::string> GetSortedFilesUnder(const std::string &folder) {
	vector<std::string> file_names;
	LocalFileSystem::CreateLocal()->ListFiles(
	    folder, [&file_names](const string &fname, bool /*unused*/) { file_names.emplace_back(fname); });
	std::sort(file_names.begin(), file_names.end());
	return file_names;
}

idx_t GetOverallFileSystemDiskSpace(const std::string &path) {
	struct statvfs vfs;

	const auto ret = statvfs(path.c_str(), &vfs);
	D_ASSERT(ret > 0);

	auto total_blocks = vfs.f_blocks;
	auto block_size = vfs.f_frsize;
	return static_cast<idx_t>(total_blocks) * static_cast<idx_t>(block_size);
}

bool CanCacheOnDisk(const std::string &path) {
	if (g_test_insufficient_disk_space) {
		return false;
	}

	const auto avai_fs_bytes = FileSystem::GetAvailableDiskSpace(path);
	if (!avai_fs_bytes.IsValid()) {
		return false;
	}

	// Check user override configurations if specified.
	if (g_min_disk_bytes_for_cache != DEFAULT_MIN_DISK_BYTES_FOR_CACHE) {
		return g_min_disk_bytes_for_cache <= avai_fs_bytes.GetIndex();
	}

	// Check default reserved disk space.
	// The function if frequently called on critical path, but filesystem metadata is highly cache-able, so the overhead
	// is just syscall.
	const idx_t overall_fs_bytes = GetOverallFileSystemDiskSpace(path);
	return overall_fs_bytes * MIN_DISK_SPACE_PERCENTAGE_FOR_CACHE <= avai_fs_bytes.GetIndex();
}

} // namespace duckdb

#include "filesystem_utils.hpp"

#include <algorithm>
#include <ctime>
#include <fstream>
#include <filesystem>
#include <iterator>

#if defined(_WIN32)
#include <windows.h>
#endif

#if !defined(_WIN32)
#include <cerrno>
#include <cstring>
#include <utime.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>
#endif

#include "cache_filesystem_config.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {
// Used to set file attribute for on-disk cache version.
// Linux requires the "user." namespace.
constexpr const char *CACHE_VERSION_ATTR_KEY = "user.cache_version";
} // namespace

vector<string> EvictStaleCacheFiles(FileSystem &local_filesystem, const string &cache_directory) {
	vector<string> evicted_cache_files;

	const timestamp_t now = Timestamp::GetCurrentTimestamp();
	local_filesystem.ListFiles(cache_directory, [&evicted_cache_files, &local_filesystem, &cache_directory,
	                                             now](const string &fname, bool /*unused*/) {
		// Multiple threads could attempt to access and delete stale files, tolerate non-existent file.
		string full_name = StringUtil::Format("%s/%s", cache_directory, fname);
		auto file_handle = local_filesystem.OpenFile(full_name, FileOpenFlags::FILE_FLAGS_READ |
		                                                            FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (file_handle == nullptr) {
			return;
		}

		const timestamp_t last_mod_time = local_filesystem.GetLastModifiedTime(*file_handle);
		const int64_t diff_in_microsec = now.value - last_mod_time.value;
		if (diff_in_microsec >= CACHE_FILE_STALENESS_MICROSEC) {
			if (std::remove(full_name.data()) < -1 && errno != EEXIST) {
				throw IOException("Fails to delete stale cache file %s", full_name);
			}
			evicted_cache_files.emplace_back(std::move(full_name));
		}
	});

	return evicted_cache_files;
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
#if defined(_WIN32)
	ULARGE_INTEGER total_bytes;
	ULARGE_INTEGER free_bytes_unused;
	ULARGE_INTEGER total_free_unused;
	const BOOL ok = GetDiskFreeSpaceExA(path.c_str(), &free_bytes_unused, &total_bytes, &total_free_unused);
	D_ASSERT(ok);
	return static_cast<idx_t>(total_bytes.QuadPart);
#else
	struct statvfs vfs;

	const auto ret = statvfs(path.c_str(), &vfs);
	D_ASSERT(ret == 0);

	auto total_blocks = vfs.f_blocks;
	auto block_size = vfs.f_frsize;
	return static_cast<idx_t>(total_blocks) * static_cast<idx_t>(block_size);
#endif
}

bool CanCacheOnDisk(const std::string &path) {
	if (g_test_insufficient_disk_space) {
		return false;
	}

	const auto avai_fs_bytes = FileSystem::GetAvailableDiskSpace(path);
	if (!avai_fs_bytes.IsValid()) {
		return false;
	}

	// If the left disk space is smaller than a cache block, there's no need to do on-disk cache.
	if (avai_fs_bytes.GetIndex() <= g_cache_block_size) {
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

map<timestamp_t, string> GetOnDiskFilesUnder(const vector<string> &folders) {
	map<timestamp_t, string> cache_files_map;
	auto local_filesystem = LocalFileSystem::CreateLocal();
	for (const auto &cur_folder : folders) {
		local_filesystem->ListFiles(cur_folder, [&local_filesystem, &cur_folder, &cache_files_map](const string &fname,
		                                                                                           bool /*unused*/) {
			// Multiple threads could attempt to access and delete stale files, tolerate non-existent file.
			const string full_name = StringUtil::Format("%s/%s", cur_folder, fname);
			auto file_handle = local_filesystem->OpenFile(full_name, FileOpenFlags::FILE_FLAGS_READ |
			                                                             FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
			if (file_handle == nullptr) {
				return;
			}

			timestamp_t last_mod_time = local_filesystem->GetLastModifiedTime(*file_handle);
			while (true) {
				auto iter = cache_files_map.find(last_mod_time);
				if (iter == cache_files_map.end()) {
					cache_files_map.emplace(last_mod_time, std::move(full_name));
					break;
				}
				// For duplicate timestamp, for simplicity simply keep incrementing until we find an available slot,
				// instead of maintaining a vector.
				last_mod_time = timestamp_t {last_mod_time.value + 1};
			}
		});
	}
	return cache_files_map;
}

bool UpdateFileTimestamps(const string &filepath) {
#if defined(_WIN32)
	// [FILE_SHARE_DELETE] is specified to allow the file to be deleted concurrently by other threads.
	HANDLE hFile =
	    CreateFileA(filepath.c_str(), FILE_WRITE_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
	                /*lpSecurityAttributes=*/nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL,
	                /*hTemplateFile=*/nullptr);
	if (hFile == INVALID_HANDLE_VALUE) {
		return false;
	}

	FILETIME ft;
	SYSTEMTIME st;
	GetSystemTime(&st);
	SystemTimeToFileTime(&st, &ft);

	BOOL success = SetFileTime(hFile, nullptr, &ft, &ft);
	CloseHandle(hFile);

	return success != 0;
#else
	const int ret_code = utime(filepath.c_str(), /*times=*/nullptr);
	return ret_code == 0;
#endif
}

bool SetCacheVersion(const string &filepath, const string &version) {
#if defined(_WIN32)
	// WINDOWS: Open the Alternate Data Stream "file.ext:streamname"
	// MSVC's implementation of std::ofstream handles ADS paths correctly.
	const string ads_path = StringUtil::Format("%s:%s", filepath, CACHE_VERSION_ATTR_KEY);
	std::ofstream ads(ads_path, std::ios::binary);
	if (!ads.is_open()) {
		return false;
	}
	ads << version;
	return ads.good();
#elif defined(__APPLE__)
	const int res = setxattr(filepath.c_str(), CACHE_VERSION_ATTR_KEY, version.c_str(), version.size(), /*position=*/0,
	                         /*options=*/0);
	return res == 0;
#elif defined(__linux__)
	const int res = setxattr(filepath.c_str(), CACHE_VERSION_ATTR_KEY, version.c_str(), version.size(), /*flags=*/0);
	return res == 0;
#else
	return false;
#endif
}

string GetCacheVersion(const string &filepath) {
#if defined(_WIN32)
	const string ads_path = StringUtil::Format("%s:%s", filepath, CACHE_VERSION_ATTR_KEY);
	std::ifstream ads(ads_path, std::ios::binary);
	if (!ads.is_open()) {
		return "";
	}
	return string((std::istreambuf_iterator<char>(ads)), std::istreambuf_iterator<char>());
#elif defined(__APPLE__)
	ssize_t size =
	    getxattr(filepath.c_str(), CACHE_VERSION_ATTR_KEY, nullptr, /*size=*/0, /*position=*/0, /*options=*/0);
	if (size <= 0) {
		return "";
	}
	string buffer(size, '\0');
	ssize_t res = getxattr(filepath.c_str(), CACHE_VERSION_ATTR_KEY,
	                       const_cast<void *>(static_cast<const void *>(buffer.data())), size, /*position=*/0,
	                       /*options=*/0);
	if (res > 0) {
		buffer.resize(res);
		return buffer;
	}
	return "";
#elif defined(__linux__)
	ssize_t size = getxattr(filepath.c_str(), CACHE_VERSION_ATTR_KEY, /*value=*/nullptr, /*size=*/0);
	if (size <= 0) {
		return "";
	}
	string buffer(size, '\0');
	ssize_t res = getxattr(filepath.c_str(), CACHE_VERSION_ATTR_KEY,
	                       const_cast<void *>(static_cast<const void *>(buffer.data())), size);
	if (res > 0) {
		buffer.resize(res);
		return buffer;
	}
	return "";
#else
	return "";
#endif
}

} // namespace duckdb

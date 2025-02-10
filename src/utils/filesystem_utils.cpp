#include "filesystem_utils.hpp"

#include <algorithm>
#include <ctime>

#include "cache_filesystem_config.hpp"
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

} // namespace duckdb

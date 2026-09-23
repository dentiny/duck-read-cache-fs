#include "disk_cache_footprint_tracker.hpp"

#include "disk_cache_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

// Get the size for the file at [filepath]; returns 0 if the file doesn't exist (i.e. deleted concurrently).
idx_t GetFileSizeIfExists(FileSystem &local_filesystem, const string &filepath) {
	auto file_handle = local_filesystem.OpenFile(filepath, FileOpenFlags::FILE_FLAGS_READ |
	                                                           FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	if (file_handle == nullptr) {
		return 0;
	}
	const int64_t file_size = local_filesystem.GetFileSize(*file_handle);
	if (file_size < 0) {
		return 0;
	}
	return NumericCast<idx_t>(file_size);
}

} // namespace

idx_t DiskCacheFootprintTracker::GetOrLoad(const vector<string> &cache_directories) {
	const concurrency::lock_guard<concurrency::mutex> lck(mutex);
	if (total_cache_file_bytes) {
		return *total_cache_file_bytes;
	}

	idx_t total_bytes = 0;
	auto local_filesystem = LocalFileSystem::CreateLocal();
	for (const auto &cur_cache_dir : cache_directories) {
		local_filesystem->ListFiles(cur_cache_dir, [&](const string &fname, bool /*unused*/) {
			// Skip in-flight temporary cache files, which are deleted after cache file finalization.
			if (DiskCacheUtil::IsTempCacheFile(fname)) {
				return;
			}
			// Tolerate concurrently deleted cache files.
			total_bytes += GetFileSizeIfExists(*local_filesystem, StringUtil::Format("%s/%s", cur_cache_dir, fname));
		});
	}
	total_cache_file_bytes = total_bytes;
	return total_bytes;
}

void DiskCacheFootprintTracker::Add(idx_t bytes) {
	const concurrency::lock_guard<concurrency::mutex> lck(mutex);
	if (!total_cache_file_bytes) {
		return;
	}
	*total_cache_file_bytes += bytes;
}

void DiskCacheFootprintTracker::Subtract(idx_t bytes) {
	const concurrency::lock_guard<concurrency::mutex> lck(mutex);
	if (!total_cache_file_bytes) {
		return;
	}
	*total_cache_file_bytes = *total_cache_file_bytes >= bytes ? *total_cache_file_bytes - bytes : 0;
}

void DiskCacheFootprintTracker::SubtractFileSize(FileSystem &local_filesystem, const string &filepath) {
	if (!IsLoaded()) {
		return;
	}
	Subtract(GetFileSizeIfExists(local_filesystem, filepath));
}

bool DiskCacheFootprintTracker::IsLoaded() const {
	const concurrency::lock_guard<concurrency::mutex> lck(mutex);
	return total_cache_file_bytes.has_value();
}

void DiskCacheFootprintTracker::Invalidate() {
	const concurrency::lock_guard<concurrency::mutex> lck(mutex);
	total_cache_file_bytes = nullopt;
}

} // namespace duckdb

#include "noop_cache_reader.hpp"

#include "cache_filesystem.hpp"

namespace duckdb {

void NoopCacheReader::ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                   idx_t requested_bytes_to_read, idx_t file_size,
                                   BaseProfileCollector *profile_collector) {
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto *internal_filesystem = disk_cache_handle.GetInternalFileSystem();
	if (profile_collector) {
		const auto latency_guard = profile_collector->RecordOperationStart(IoOperation::kRead);
		internal_filesystem->Read(*disk_cache_handle.internal_file_handle, buffer, requested_bytes_to_read,
		                          requested_start_offset);
	} else {
		internal_filesystem->Read(*disk_cache_handle.internal_file_handle, buffer, requested_bytes_to_read,
		                          requested_start_offset);
	}
}

} // namespace duckdb

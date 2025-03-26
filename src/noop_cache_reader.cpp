#include "noop_cache_reader.hpp"

#include "cache_filesystem.hpp"

namespace duckdb {

void NoopCacheReader::ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                   idx_t requested_bytes_to_read, idx_t file_size) {
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
	auto *internal_filesystem = disk_cache_handle.GetInternalFileSystem();
	const string oper_id = profile_collector->GenerateOperId();
	profile_collector->RecordOperationStart(BaseProfileCollector::IoOperation::kRead, oper_id);
	internal_filesystem->Read(*disk_cache_handle.internal_file_handle, buffer, requested_bytes_to_read,
	                          requested_start_offset);
	profile_collector->RecordOperationEnd(BaseProfileCollector::IoOperation::kRead, oper_id);
}

} // namespace duckdb

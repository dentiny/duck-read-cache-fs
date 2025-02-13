#include "base_cache_filesystem.hpp"
#include "noop_cache_reader.hpp"

namespace duckdb {

void NoopCacheReader::ReadAndCache(FileHandle &handle, char *buffer, idx_t requested_start_offset,
                                   idx_t requested_bytes_to_read, idx_t file_size) {
	auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
	const string cur_oper = StringUtil::Format("%d", requested_start_offset);
	profile_collector->RecordOperationStart(cur_oper);
	internal_filesystem->Read(*disk_cache_handle.internal_file_handle, buffer, requested_bytes_to_read,
	                          requested_start_offset);
	profile_collector->RecordOperationEnd(cur_oper);
}

} // namespace duckdb

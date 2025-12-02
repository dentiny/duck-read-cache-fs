#include "cache_status_query_function.hpp"

#include <algorithm>

#include "cache_entry_info.hpp"
#include "cache_filesystem.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Data cache status query function
//===--------------------------------------------------------------------===//

struct DataCacheStatusData : public GlobalTableFunctionState {
	vector<DataCacheEntryInfo> cache_entries_info;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

unique_ptr<FunctionData> DataCacheStatusQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(5);
	names.reserve(5);

	// Cache filepath.
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("cache_filepath");

	// Remote object name.
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("remote_filename");

	// Start offset for cache file.
	return_types.emplace_back(LogicalType {LogicalTypeId::UBIGINT});
	names.emplace_back("start_offset");

	// End offset for cache file.
	return_types.emplace_back(LogicalType {LogicalTypeId::UBIGINT});
	names.emplace_back("end_offset");

	// Cache type.
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("cache_type");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DataCacheStatusQueryFuncInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto result = make_uniq<DataCacheStatusData>();
	auto &entries_info = result->cache_entries_info;

	// Get instance state
	auto inst_state = GetInstanceStateShared(*context.db);
	if (inst_state) {
		// Initialize disk cache reader to access on-disk cache file, even if it's not initialized before.
		inst_state->cache_reader_manager.InitializeDiskCacheReader(inst_state->config.on_disk_cache_directories,
		                                                           inst_state);

		// Get cache entries information from all cache filesystems and all initialized cache readers.
		auto cache_readers = inst_state->cache_reader_manager.GetCacheReaders();
		for (auto *cur_cache_reader : cache_readers) {
			auto cache_entries_info = cur_cache_reader->GetCacheEntriesInfo();
			entries_info.reserve(entries_info.size() + cache_entries_info.size());

			for (auto &cur_cache_info : cache_entries_info) {
				entries_info.emplace_back(std::move(cur_cache_info));
			}
		}
	}

	// Sort the cache entries info for better visibility.
	std::sort(entries_info.begin(), entries_info.end());

	return std::move(result);
}

void DataCacheStatusQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DataCacheStatusData>();

	// All entries have been emitted.
	if (data.offset >= data.cache_entries_info.size()) {
		return;
	}

	// Start filling in the result buffer.
	idx_t count = 0;
	while (data.offset < data.cache_entries_info.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.cache_entries_info[data.offset++];
		idx_t col = 0;

		// Cache filepath.
		output.SetValue(col++, count, entry.cache_filepath);

		// Remote filename.
		output.SetValue(col++, count, entry.remote_filename);

		// Start offset.
		output.SetValue(col++, count, Value::BIGINT(NumericCast<uint64_t>(entry.start_offset)));

		// End offset.
		output.SetValue(col++, count, Value::BIGINT(NumericCast<uint64_t>(entry.end_offset)));

		// Cache type.
		output.SetValue(col++, count, entry.cache_type);

		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Cache access information query function
//===--------------------------------------------------------------------===//

struct CacheAccessInfoData : public GlobalTableFunctionState {
	// Index-ed by [CacheEntity].
	vector<CacheAccessInfo> cache_access_info;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

unique_ptr<FunctionData> CacheAccessInfoQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(6);
	names.reserve(6);

	// Cache type.
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("cache_type");

	// Cache hit count.
	return_types.emplace_back(LogicalType {LogicalTypeId::UBIGINT});
	names.emplace_back("cache_hit_count");

	// Cache miss count.
	return_types.emplace_back(LogicalType {LogicalTypeId::UBIGINT});
	names.emplace_back("cache_miss_count");

	// Used for file handle cache, cache miss by in-use count.
	return_types.emplace_back(LogicalType {LogicalTypeId::UBIGINT});
	names.emplace_back("cache_miss_by_in_use (file handle cache)");

	// Used for data cache, total number of bytes to read.
	return_types.emplace_back(LogicalType {LogicalTypeId::UBIGINT});
	names.emplace_back("number_bytes_to_read (data cache)");

	// Used for data cache, total number of bytes to cache.
	return_types.emplace_back(LogicalType {LogicalTypeId::UBIGINT});
	names.emplace_back("number_bytes_to_cache (data cache)");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> CacheAccessInfoQueryFuncInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto result = make_uniq<CacheAccessInfoData>();
	auto &aggregated_cache_access_infos = result->cache_access_info;
	aggregated_cache_access_infos.resize(kCacheEntityCount);

	// Set cache type, because there could be no cache readers available.
	for (idx_t idx = 0; idx < kCacheEntityCount; ++idx) {
		aggregated_cache_access_infos[idx].cache_type = CACHE_ENTITY_NAMES[idx];
	}

	// Get cache access info from all initialized cache readers.
	auto *inst_state = GetInstanceState(*context.db);
	if (!inst_state) {
		return std::move(result);
	}
	const auto cache_readers = inst_state->cache_reader_manager.GetCacheReaders();
	for (auto *cur_cache_reader : cache_readers) {
		auto *profiler_collector = cur_cache_reader->GetProfileCollector();
		if (profiler_collector == nullptr) {
			continue;
		}
		auto cache_access_info = profiler_collector->GetCacheAccessInfo();
		D_ASSERT(cache_access_info.size() == kCacheEntityCount);
		for (idx_t idx = 0; idx < kCacheEntityCount; ++idx) {
			auto &cur_cache_access_info = cache_access_info[idx];
			aggregated_cache_access_infos[idx].cache_hit_count += cur_cache_access_info.cache_hit_count;
			aggregated_cache_access_infos[idx].cache_miss_count += cur_cache_access_info.cache_miss_count;
			aggregated_cache_access_infos[idx].cache_miss_by_in_use += cur_cache_access_info.cache_miss_by_in_use;

			// For data file cache, record number of bytes to read and to cache.
			if (idx == static_cast<idx_t>(IoOperation::kRead)) {
				// Handle number of bytes to read.
				auto &total_bytes_to_read = aggregated_cache_access_infos[idx].total_bytes_to_read;
				uint64_t read_value = 0;
				if (!total_bytes_to_read.IsNull()) {
					read_value = total_bytes_to_read.GetValue<uint64_t>();
				}
				total_bytes_to_read =
				    Value::UBIGINT(read_value + cur_cache_access_info.total_bytes_to_read.GetValue<uint64_t>());

				// Handle number of bytes to cache.
				auto &total_bytes_to_cache = aggregated_cache_access_infos[idx].total_bytes_to_cache;
				uint64_t cache_value = 0;
				if (!total_bytes_to_cache.IsNull()) {
					cache_value = total_bytes_to_cache.GetValue<uint64_t>();
				}
				total_bytes_to_cache =
				    Value::UBIGINT(cache_value + cur_cache_access_info.total_bytes_to_cache.GetValue<uint64_t>());
			}
		}
	}

	return std::move(result);
}

void CacheAccessInfoQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<CacheAccessInfoData>();

	// All entries have been emitted.
	if (data.offset >= data.cache_access_info.size()) {
		return;
	}

	// Start filling in the result buffer.
	idx_t count = 0;
	while (data.offset < data.cache_access_info.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.cache_access_info[data.offset++];
		idx_t col = 0;

		// Cache type.
		output.SetValue(col++, count, entry.cache_type);

		// Cache hit count.
		output.SetValue(col++, count, Value::BIGINT(NumericCast<uint64_t>(entry.cache_hit_count)));

		// Cache miss count.
		output.SetValue(col++, count, Value::BIGINT(NumericCast<uint64_t>(entry.cache_miss_count)));

		// Cache miss by in-use count.
		output.SetValue(col++, count, Value::BIGINT(NumericCast<uint64_t>(entry.cache_miss_by_in_use)));

		// Used for data cache, total number of bytes to read.
		output.SetValue(col++, count, entry.total_bytes_to_read);

		// Used for data cache, total number of bytes to cache.
		output.SetValue(col++, count, entry.total_bytes_to_cache);

		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Wrapped cache filesystem query function
//===--------------------------------------------------------------------===//
struct WrappedFilesystemsData : public GlobalTableFunctionState {
	vector<string> wrapped_filesystems;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

unique_ptr<FunctionData> WrappedCacheFileSystemsFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(1);
	names.reserve(1);

	// Wrapped cache filesystem name.
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("wrapped_filesystems");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> WrappedCacheFileSystemsFuncInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto result = make_uniq<WrappedFilesystemsData>();
	auto &wrapped_filesystems = result->wrapped_filesystems;

	// Get cache filesystems from per-instance registry
	auto *state = GetInstanceState(*context.db);
	if (state) {
		auto cache_filesystem_instances = state->registry.GetAllCacheFs();
		wrapped_filesystems.reserve(cache_filesystem_instances.size());

		for (auto *cur_cache_fs : cache_filesystem_instances) {
			wrapped_filesystems.emplace_back(cur_cache_fs->GetInternalFileSystem()->GetName());
		}
	}

	return std::move(result);
}

void WrappedCacheFileSystemsTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<WrappedFilesystemsData>();

	// All entries have been emitted.
	if (data.offset >= data.wrapped_filesystems.size()) {
		return;
	}

	// Start filling in the result buffer.
	idx_t count = 0;
	while (data.offset < data.wrapped_filesystems.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.wrapped_filesystems[data.offset++];
		idx_t col = 0;

		// Wrapped cache filesystem name.
		output.SetValue(col, count, entry);

		count++;
	}
	output.SetCardinality(count);
}
} // namespace

TableFunction GetDataCacheStatusQueryFunc() {
	TableFunction data_cache_status_query_func {/*name=*/"cache_httpfs_cache_status_query",
	                                            /*arguments=*/ {},
	                                            /*function=*/DataCacheStatusQueryTableFunc,
	                                            /*bind=*/DataCacheStatusQueryFuncBind,
	                                            /*init_global=*/DataCacheStatusQueryFuncInit};
	return data_cache_status_query_func;
}

TableFunction GetCacheAccessInfoQueryFunc() {
	TableFunction cache_access_info_query_func {/*name=*/"cache_httpfs_cache_access_info_query",
	                                            /*arguments=*/ {},
	                                            /*function=*/CacheAccessInfoQueryTableFunc,
	                                            /*bind=*/CacheAccessInfoQueryFuncBind,
	                                            /*init_global=*/CacheAccessInfoQueryFuncInit};
	return cache_access_info_query_func;
}

TableFunction GetWrappedCacheFileSystemsFunc() {
	TableFunction wrapped_cache_filesystems_query_func {/*name=*/"cache_httpfs_get_cache_filesystems",
	                                                    /*arguments=*/ {},
	                                                    /*function=*/WrappedCacheFileSystemsTableFunc,
	                                                    /*bind=*/WrappedCacheFileSystemsFuncBind,
	                                                    /*init_global=*/WrappedCacheFileSystemsFuncInit};
	return wrapped_cache_filesystems_query_func;
}
} // namespace duckdb

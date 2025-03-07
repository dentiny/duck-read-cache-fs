#include "cache_status_query_function.hpp"

#include <algorithm>

#include "cache_entry_info.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_ref_registry.hpp"
#include "cache_reader_manager.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

namespace {

struct CacheStatusData : public GlobalTableFunctionState {
	vector<CacheEntryInfo> cache_entries_info;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

unique_ptr<FunctionData> CacheStatusQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(5);
	names.reserve(5);

	// Cache filepath.
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("cache_filepath");

	// Remote object name.
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("remote_filename");

	// Start offset for cache file.
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("start_offset");

	// End offset for cache file.
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("end_offset");

	// Cache type.
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("cache_type");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> CacheStatusQueryFuncInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<CacheStatusData>();
	auto &entries_info = result->cache_entries_info;

	// Initialize disk cache reader to access on-disk cache file, even if it's not initialized before.
	auto &cache_reader_manager = CacheReaderManager::Get();
	cache_reader_manager.InitializeDiskCacheReader();

	// Get cache entries information from all cache filesystems and all initialized cache readers.
	auto cache_readers = cache_reader_manager.GetCacheReaders();
	for (auto *cur_cache_reader : cache_readers) {
		auto cache_entries_info = cur_cache_reader->GetCacheEntriesInfo();
		entries_info.reserve(entries_info.size() + cache_entries_info.size());

		for (auto &cur_cache_info : cache_entries_info) {
			entries_info.emplace_back(std::move(cur_cache_info));
		}
	}

	// Sort the cache entries info for better visibility.
	std::sort(entries_info.begin(), entries_info.end());

	return std::move(result);
}

void CacheStatusQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<CacheStatusData>();

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

} // namespace

TableFunction GetCacheStatusQueryFunc() {
	TableFunction cache_status_query_func {/*name=*/"cache_httpfs_cache_status_query",
	                                       /*arguments=*/ {},
	                                       /*function=*/CacheStatusQueryTableFunc,
	                                       /*bind=*/CacheStatusQueryFuncBind,
	                                       /*init_global=*/CacheStatusQueryFuncInit};
	return cache_status_query_func;
}

} // namespace duckdb

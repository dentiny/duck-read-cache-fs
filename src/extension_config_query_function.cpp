#include "extension_config_query_function.hpp"

#include "cache_exclusion_manager.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

namespace {

struct CacheConfigData : public GlobalTableFunctionState {
	// Cache config should be emitted only once.
	bool emitted = false;
};

// Get disk cache directories in duckdb [`Value`].
Value GetDiskCacheDirectories() {
	vector<Value> directories;
	directories.reserve(g_on_disk_cache_directories->size());
	for (const auto &cur_dir : *g_on_disk_cache_directories) {
		directories.emplace_back(Value {cur_dir});
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(directories));
}

// Get cache exclusion regexes in duckdb [`Value`].
Value GetCacheExclusionRegexes() {
	auto exclusion_regexes = CacheExclusionManager::GetInstance().GetExclusionRegex();
	vector<Value> exclusion_regex_values;
	exclusion_regex_values.reserve(exclusion_regexes.size());
	for (auto &cur_regex : exclusion_regex_values) {
		exclusion_regex_values.emplace_back(Value {std::move(cur_regex)});
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(exclusion_regex_values));
}

void DataCacheConfigQueryFuncBindImpl(vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("data cache type");
	if (*g_cache_type == *ON_DISK_CACHE_TYPE) {
		return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
		names.emplace_back("disk cache directories");

		return_types.emplace_back(LogicalType::UBIGINT);
		names.emplace_back("disk cache block size");

		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back("disk cache eviction policy");
	} else if (*g_cache_type == *IN_MEM_CACHE_TYPE) {
		return_types.emplace_back(LogicalType::UBIGINT);
		names.emplace_back("in-memory cache block size");

		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back("in-memory cache eviction policy");
	}
}

void FillDataCacheConfig(DataChunk &output, idx_t &col) {
	output.SetValue(col++, /*index=*/0, *g_cache_type);
	if (*g_cache_type == *ON_DISK_CACHE_TYPE) {
		output.SetValue(col++, /*index=*/0, GetDiskCacheDirectories());
		output.SetValue(col++, /*index=*/0, Value::UBIGINT(g_cache_block_size));
		output.SetValue(col++, /*index=*/0, *g_on_disk_eviction_policy);
	} else if (*g_cache_type == *IN_MEM_CACHE_TYPE) {
		output.SetValue(col++, /*index=*/0, Value::UBIGINT(g_cache_block_size));
		output.SetValue(col++, /*index=*/0, "lru"); // currently only LRU supported
	}
}

void MetadataCacheConfigQueryFuncBindImpl(vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("metadata cache type");
	if (g_enable_metadata_cache) {
		return_types.emplace_back(LogicalType::UBIGINT);
		names.emplace_back("metadata cache entry size");
	}
}

void FillMetadataCacheConfig(DataChunk &output, idx_t &col) {
	if (g_enable_metadata_cache) {
		output.SetValue(col++, /*index=*/0, "enabled");
		output.SetValue(col++, /*index=*/0, Value::UBIGINT(g_max_metadata_cache_entry));
	} else {
		output.SetValue(col++, /*index=*/0, "disabled");
	}
}

void FileHandleCacheConfigQueryFuncBindImpl(vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file handle cache type");
	if (g_enable_file_handle_cache) {
		return_types.emplace_back(LogicalType::UBIGINT);
		names.emplace_back("file handle cache entry size");
	}
}

void FillFileHandleCacheConfig(DataChunk &output, idx_t &col) {
	if (g_enable_file_handle_cache) {
		output.SetValue(col++, /*index=*/0, "enabled");
		output.SetValue(col++, /*index=*/0, Value::UBIGINT(g_max_file_handle_cache_entry));
	} else {
		output.SetValue(col++, /*index=*/0, "disabled");
	}
}

void GlobCacheConfigQueryFuncBindImpl(vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("glob cache type");
	if (g_enable_glob_cache) {
		return_types.emplace_back(LogicalType::UBIGINT);
		names.emplace_back("glob cache entry size");
	}
}

void FillGlobCacheConfig(DataChunk &output, idx_t &col) {
	if (g_enable_glob_cache) {
		output.SetValue(col++, /*index=*/0, "enabled");
		output.SetValue(col++, /*index=*/0, Value::UBIGINT(g_max_glob_cache_entry));
	} else {
		output.SetValue(col++, /*index=*/0, "disabled");
	}
}

//===--------------------------------------------------------------------===//
// Data cache config query function
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> DataCacheConfigQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());
	DataCacheConfigQueryFuncBindImpl(return_types, names);
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DataCacheConfigQueryFuncInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto result = make_uniq<CacheConfigData>();
	return std::move(result);
}

void DataCacheConfigQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<CacheConfigData>();

	// Config has already been emitted.
	if (data.emitted) {
		return;
	}
	data.emitted = true;

	idx_t col = 0;
	FillDataCacheConfig(output, col);
	output.SetCardinality(/*count=*/1);
}

//===--------------------------------------------------------------------===//
// Metadata cache config query function
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> MetadataCacheConfigQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());
	MetadataCacheConfigQueryFuncBindImpl(return_types, names);
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> MetadataCacheConfigQueryFuncInit(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto result = make_uniq<CacheConfigData>();
	return std::move(result);
}

void MetadataCacheConfigQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<CacheConfigData>();

	// Config has already been emitted.
	if (data.emitted) {
		return;
	}
	data.emitted = true;

	idx_t col = 0;
	FillMetadataCacheConfig(output, col);
	output.SetCardinality(/*count=*/1);
}

//===--------------------------------------------------------------------===//
// File handle cache config query function
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> FileHandleCacheConfigQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());
	FileHandleCacheConfigQueryFuncBindImpl(return_types, names);
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> FileHandleCacheConfigQueryFuncInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto result = make_uniq<CacheConfigData>();
	return std::move(result);
}

void FileHandleCacheConfigQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<CacheConfigData>();

	// Config has already been emitted.
	if (data.emitted) {
		return;
	}
	data.emitted = true;

	idx_t col = 0;
	FillFileHandleCacheConfig(output, col);
	output.SetCardinality(/*count=*/1);
}

//===--------------------------------------------------------------------===//
// Glob cache config query function
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> GlobCacheConfigQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());
	GlobCacheConfigQueryFuncBindImpl(return_types, names);
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> GlobCacheConfigQueryFuncInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto result = make_uniq<CacheConfigData>();
	return std::move(result);
}

void GlobCacheConfigQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<CacheConfigData>();

	// Config has already been emitted.
	if (data.emitted) {
		return;
	}
	data.emitted = true;

	idx_t col = 0;
	FillGlobCacheConfig(output, col);
	output.SetCardinality(/*count=*/1);
}

//===--------------------------------------------------------------------===//
// Cache type and enablement query function
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> CacheTypeQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(4);
	names.reserve(4);

	// Intentionally use string instead of boolean to indicate cache enabled or not, for better display.
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("data cache");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("metadata cache");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file handle cache");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("glob cache");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> CacheTypeQueryFuncInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<CacheConfigData>();
	return std::move(result);
}

void CacheTypeQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<CacheConfigData>();

	// Config has already been emitted.
	if (data.emitted) {
		return;
	}
	data.emitted = true;

	idx_t col = 0;

	// Data cache.
	output.SetValue(col++, /*index=*/0, *g_cache_type);

	// Metadata cache.
	if (g_enable_metadata_cache) {
		output.SetValue(col++, /*index=*/0, "enabled");
	} else {
		output.SetValue(col++, /*index=*/0, "disabled");
	}

	// File handle cache.
	if (g_enable_file_handle_cache) {
		output.SetValue(col++, /*index=*/0, "enabled");
	} else {
		output.SetValue(col++, /*index=*/0, "disabled");
	}

	// Glob cache.
	if (g_enable_glob_cache) {
		output.SetValue(col++, /*index=*/0, "enabled");
	} else {
		output.SetValue(col++, /*index=*/0, "disabled");
	}

	output.SetCardinality(/*count=*/1);
}

//===--------------------------------------------------------------------===//
// Cache config query function
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> CacheConfigQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	// Data cache config.
	DataCacheConfigQueryFuncBindImpl(return_types, names);

	// Metadata cache config.
	MetadataCacheConfigQueryFuncBindImpl(return_types, names);

	// File handle cache config.
	FileHandleCacheConfigQueryFuncBindImpl(return_types, names);

	// Glob cache config.
	GlobCacheConfigQueryFuncBindImpl(return_types, names);

	// Cache exclusion regex.
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
	names.emplace_back("cache exclusion regexes");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> CacheConfigQueryFuncInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<CacheConfigData>();
	return std::move(result);
}

void CacheConfigQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<CacheConfigData>();

	// Config has already been emitted.
	if (data.emitted) {
		return;
	}
	data.emitted = true;

	idx_t col = 0;

	// Data cache config.
	FillDataCacheConfig(output, col);

	// Metadata cache config.
	FillMetadataCacheConfig(output, col);

	// File handle cache config.
	FillFileHandleCacheConfig(output, col);

	// Glob cache config.
	FillGlobCacheConfig(output, col);

	// Cache exclusion regex.
	output.SetValue(col++, /*index=*/0, GetCacheExclusionRegexes());

	output.SetCardinality(/*count=*/1);
}
} // namespace

TableFunction GetDataCacheConfigQueryFunc() {
	TableFunction get_data_cache_config_query_func {/*name=*/"cache_httpfs_get_data_cache_config",
	                                                /*arguments=*/ {},
	                                                /*function=*/DataCacheConfigQueryTableFunc,
	                                                /*bind=*/DataCacheConfigQueryFuncBind,
	                                                /*init_global=*/DataCacheConfigQueryFuncInit};
	return get_data_cache_config_query_func;
}

TableFunction GetMetadataCacheConfigQueryFunc() {
	TableFunction get_metadata_cache_config_query_func {/*name=*/"cache_httpfs_get_metadata_cache_config",
	                                                    /*arguments=*/ {},
	                                                    /*function=*/MetadataCacheConfigQueryTableFunc,
	                                                    /*bind=*/MetadataCacheConfigQueryFuncBind,
	                                                    /*init_global=*/MetadataCacheConfigQueryFuncInit};
	return get_metadata_cache_config_query_func;
}

TableFunction GetFileHandleCacheConfigQueryFunc() {
	TableFunction get_file_handle_cache_config_query_func {/*name=*/"cache_httpfs_get_file_handle_cache_config",
	                                                       /*arguments=*/ {},
	                                                       /*function=*/FileHandleCacheConfigQueryTableFunc,
	                                                       /*bind=*/FileHandleCacheConfigQueryFuncBind,
	                                                       /*init_global=*/FileHandleCacheConfigQueryFuncInit};
	return get_file_handle_cache_config_query_func;
}

TableFunction GetGlobCacheConfigQueryFunc() {
	TableFunction get_glob_cache_config_query_func {/*name=*/"cache_httpfs_get_glob_cache_config",
	                                                /*arguments=*/ {},
	                                                /*function=*/GlobCacheConfigQueryTableFunc,
	                                                /*bind=*/GlobCacheConfigQueryFuncBind,
	                                                /*init_global=*/GlobCacheConfigQueryFuncInit};
	return get_glob_cache_config_query_func;
}

TableFunction GetCacheTypeQueryFunc() {
	TableFunction get_cache_type_query_func {/*name=*/"cache_httpfs_get_cache_type",
	                                         /*arguments=*/ {},
	                                         /*function=*/CacheTypeQueryTableFunc,
	                                         /*bind=*/CacheTypeQueryFuncBind,
	                                         /*init_global=*/CacheTypeQueryFuncInit};
	return get_cache_type_query_func;
}

TableFunction GetCacheConfigQueryFunc() {
	TableFunction get_cache_config_query_func {/*name=*/"cache_httpfs_get_cache_config",
	                                           /*arguments=*/ {},
	                                           /*function=*/CacheConfigQueryTableFunc,
	                                           /*bind=*/CacheConfigQueryFuncBind,
	                                           /*init_global=*/CacheConfigQueryFuncInit};
	return get_cache_config_query_func;
}

} // namespace duckdb

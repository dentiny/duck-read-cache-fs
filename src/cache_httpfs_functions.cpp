#include "cache_httpfs_functions.hpp"

#include "base_profile_collector.hpp"
#include "cache_exclusion_utils.hpp"
#include "cache_filesystem.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "cache_status_query_function.hpp"
#include "disk_cache_util.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "extension_config_query_function.hpp"
#include "filesystem_status_query_function.hpp"

namespace duckdb {

namespace {

constexpr bool SUCCESS = true;

DatabaseInstance &GetDatabaseInstance(ExpressionState &state) {
	auto *executor = state.root.executor;
	auto &client_context = executor->GetContext();
	return *client_context.db.get();
}

connection_t GetConnectionId(ExpressionState &state) {
	auto *executor = state.root.executor;
	auto &client_context = executor->GetContext();
	return client_context.GetConnectionId();
}

void ClearAllCache(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto &instance = GetDatabaseInstance(state);
	auto &inst_state = GetInstanceStateOrThrow(instance);

	auto local_filesystem = LocalFileSystem::CreateLocal();
	for (const auto &cur_cache_dir : inst_state.config.on_disk_cache_directories) {
		local_filesystem->RemoveDirectory(cur_cache_dir);
		local_filesystem->CreateDirectory(cur_cache_dir);
	}

	inst_state.cache_reader_manager.ClearCache();

	auto cache_filesystem_instances = inst_state.registry.GetAllCacheFs();
	for (auto *cur_cache_fs : cache_filesystem_instances) {
		cur_cache_fs->ClearCache();
	}

	auto conn_id = GetConnectionId(state);
	inst_state.profile_collector_manager.ResetProfileCollector(conn_id);

	result.Reference(Value(SUCCESS));
}

void ClearCacheForFile(const DataChunk &args, ExpressionState &state, Vector &result) {
	ALWAYS_ASSERT(args.ColumnCount() == 1);
	const string filepath = args.GetValue(/*col_idx=*/0, /*index=*/0).ToString();

	auto &instance = GetDatabaseInstance(state);
	auto &inst_state = GetInstanceStateOrThrow(instance);

	inst_state.cache_reader_manager.ClearCache(filepath);

	auto conn_id = GetConnectionId(state);
	auto cache_filesystem_instances = inst_state.registry.GetAllCacheFs();
	for (auto *cur_cache_fs : cache_filesystem_instances) {
		cur_cache_fs->ClearCache(filepath, conn_id);
	}

	result.Reference(Value(SUCCESS));
}

void CleanupDeadTemp(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto &instance = GetDatabaseInstance(state);
	auto &inst_state = GetInstanceStateOrThrow(instance);
	const idx_t deleted = DiskCacheUtil::CleanupDeadTempFiles(inst_state.config.on_disk_cache_directories, &instance,
	                                                          inst_state.config.parallel_read_mode);
	result.Reference(Value::BIGINT(NumericCast<int64_t>(deleted)));
}

void GetOnDiskDataCacheSize(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto &instance = GetDatabaseInstance(state);
	auto &inst_state = GetInstanceStateOrThrow(instance);

	auto local_filesystem = LocalFileSystem::CreateLocal();
	int64_t total_cache_size = 0;
	for (const auto &cur_cache_dir : inst_state.config.on_disk_cache_directories) {
		local_filesystem->ListFiles(cur_cache_dir, [&local_filesystem, &total_cache_size,
		                                            &cur_cache_dir](const string &fname, bool /*unused*/) {
			const string file_path = StringUtil::Format("%s/%s", cur_cache_dir, fname);
			auto file_handle = local_filesystem->OpenFile(file_path, FileOpenFlags::FILE_FLAGS_READ);
			total_cache_size += local_filesystem->GetFileSize(*file_handle);
		});
	}
	result.Reference(Value(total_cache_size));
}

void GetProfileStats(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto &instance = GetDatabaseInstance(state);
	auto &inst_state = GetInstanceStateOrThrow(instance);
	auto conn_id = GetConnectionId(state);

	if (!inst_state.profile_collector_manager.HasExplicitProfileCollector(conn_id)) {
		result.Reference(Value("No valid access to cache filesystem"));
		return;
	}

	auto &collector = inst_state.profile_collector_manager.GetProfileCollectorOrDefault(conn_id);
	auto stats_pair = collector.GetHumanReadableStats();
	auto &latest_stat = stats_pair.first;
	if (latest_stat.empty()) {
		latest_stat = "No valid access to cache filesystem";
	}
	result.Reference(Value(std::move(latest_stat)));
}

void ResetProfileStats(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto &instance = GetDatabaseInstance(state);
	auto &inst_state = GetInstanceStateOrThrow(instance);
	auto conn_id = GetConnectionId(state);
	inst_state.profile_collector_manager.ResetProfileCollector(conn_id);
	result.Reference(Value(SUCCESS));
}

void WrapCacheFileSystem(const DataChunk &args, ExpressionState &state, Vector &result) {
	ALWAYS_ASSERT(args.ColumnCount() == 1);
	const string filesystem_name = args.GetValue(/*col_idx=*/0, /*index=*/0).ToString();

	auto &duckdb_instance = GetDatabaseInstance(state);
	auto &opener_filesystem = duckdb_instance.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	auto internal_filesystem = vfs.ExtractSubSystem(filesystem_name);
	if (internal_filesystem == nullptr) {
		throw InvalidInputException("Filesystem %s hasn't been registered yet! Use "
		                            "cache_httpfs_list_registered_filesystems() to see available filesystems.",
		                            filesystem_name);
	}

	auto cache_filesystem =
	    make_uniq<CacheFileSystem>(std::move(internal_filesystem), GetInstanceStateShared(duckdb_instance));
	vfs.RegisterSubSystem(std::move(cache_filesystem));
	DUCKDB_LOG_DEBUG(duckdb_instance, StringUtil::Format("Wrap filesystem %s with cache filesystem.", filesystem_name));

	result.Reference(Value(SUCCESS));
}

FunctionDescription GetFunctionDescription(vector<string> parameter_names, string description, vector<string> examples,
                                           vector<string> categories) {
	FunctionDescription result;
	result.parameter_names = std::move(parameter_names);
	result.description = std::move(description);
	result.examples = std::move(examples);
	result.categories = std::move(categories);
	return result;
}

void RegisterScalarFunction(ExtensionLoader &loader, ScalarFunction function, vector<string> parameter_names,
                            string description, vector<string> examples, vector<string> categories) {
	CreateScalarFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(GetFunctionDescription(std::move(parameter_names), std::move(description),
	                                                   std::move(examples), std::move(categories)));
	loader.RegisterFunction(std::move(info));
}

void RegisterTableFunction(ExtensionLoader &loader, TableFunction function, string description, vector<string> examples,
                           vector<string> categories) {
	CreateTableFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(
	    GetFunctionDescription({}, std::move(description), std::move(examples), std::move(categories)));
	loader.RegisterFunction(std::move(info));
}

} // namespace

void RegisterCacheHttpfsFunctions(ExtensionLoader &loader) {
	RegisterScalarFunction(loader,
	                       ScalarFunction("cache_httpfs_add_exclusion_regex", {LogicalType::VARCHAR},
	                                      LogicalType::BOOLEAN, AddCacheExclusionRegex),
	                       {"regex"}, "Adds a regular expression for remote paths that should bypass the cache.",
	                       {"SELECT cache_httpfs_add_exclusion_regex('.*\\.tmp$');"},
	                       {"cache_httpfs", "configuration"});
	RegisterScalarFunction(
	    loader,
	    ScalarFunction("cache_httpfs_reset_exclusion_regex", {}, LogicalType::BOOLEAN, ResetCacheExclusionRegex), {},
	    "Removes all path exclusion regular expressions.", {"SELECT cache_httpfs_reset_exclusion_regex();"},
	    {"cache_httpfs", "configuration"});
	RegisterTableFunction(loader, ListCacheExclusionRegex(), "Lists the path exclusion regular expressions.",
	                      {"SELECT * FROM cache_httpfs_list_exclusion_regex();"}, {"cache_httpfs", "configuration"});

	RegisterScalarFunction(loader, ScalarFunction("cache_httpfs_clear_cache", {}, LogicalType::BOOLEAN, ClearAllCache),
	                       {}, "Clears all data, metadata, file handle, glob, and profile caches.",
	                       {"SELECT cache_httpfs_clear_cache();"}, {"cache_httpfs", "cache"});
	RegisterScalarFunction(loader,
	                       ScalarFunction("cache_httpfs_clear_cache_for_file", {LogicalType::VARCHAR},
	                                      LogicalType::BOOLEAN, ClearCacheForFile),
	                       {"filename"}, "Clears cached entries for one remote file.",
	                       {"SELECT cache_httpfs_clear_cache_for_file('s3://bucket/file.parquet');"},
	                       {"cache_httpfs", "cache"});
	RegisterScalarFunction(loader,
	                       ScalarFunction("cache_httpfs_wrap_cache_filesystem", {LogicalType::VARCHAR},
	                                      LogicalType::BOOLEAN, WrapCacheFileSystem),
	                       {"filesystem_name"}, "Wraps a registered DuckDB filesystem with the cache filesystem.",
	                       {"SELECT cache_httpfs_wrap_cache_filesystem('AzureBlobStorageFileSystem');"},
	                       {"cache_httpfs", "filesystem"});
	RegisterScalarFunction(loader,
	                       ScalarFunction("cache_httpfs_cleanup_dead_temp", {}, LogicalType::BIGINT, CleanupDeadTemp),
	                       {}, "Deletes stale temporary cache files and returns the number deleted.",
	                       {"SELECT cache_httpfs_cleanup_dead_temp();"}, {"cache_httpfs", "maintenance"});
	RegisterScalarFunction(
	    loader,
	    ScalarFunction("cache_httpfs_get_ondisk_data_cache_size", {}, LogicalType::BIGINT, GetOnDiskDataCacheSize), {},
	    "Returns the total size in bytes of files in the configured on-disk cache directories.",
	    {"SELECT cache_httpfs_get_ondisk_data_cache_size();"}, {"cache_httpfs", "observability"});
	RegisterTableFunction(loader, GetDataCacheStatusQueryFunc(),
	                      "Returns cached data entries with their local path, remote path, byte range, and cache type.",
	                      {"SELECT * FROM cache_httpfs_cache_status_query();"}, {"cache_httpfs", "observability"});

	RegisterScalarFunction(loader,
	                       ScalarFunction("cache_httpfs_get_profile", {}, LogicalType::VARCHAR, GetProfileStats), {},
	                       "Returns human-readable cache profile statistics for the current connection.",
	                       {"SELECT cache_httpfs_get_profile();"}, {"cache_httpfs", "observability"});
	RegisterScalarFunction(loader,
	                       ScalarFunction("cache_httpfs_clear_profile", {}, LogicalType::BOOLEAN, ResetProfileStats),
	                       {}, "Clears cache profile statistics for the current connection.",
	                       {"SELECT cache_httpfs_clear_profile();"}, {"cache_httpfs", "observability"});

	RegisterTableFunction(loader, GetDataCacheConfigQueryFunc(), "Returns the current data cache configuration.",
	                      {"SELECT * FROM cache_httpfs_get_data_cache_config();"}, {"cache_httpfs", "configuration"});
	RegisterTableFunction(
	    loader, GetMetadataCacheConfigQueryFunc(), "Returns the current metadata cache configuration.",
	    {"SELECT * FROM cache_httpfs_get_metadata_cache_config();"}, {"cache_httpfs", "configuration"});
	RegisterTableFunction(
	    loader, GetFileHandleCacheConfigQueryFunc(), "Returns the current file handle cache configuration.",
	    {"SELECT * FROM cache_httpfs_get_file_handle_cache_config();"}, {"cache_httpfs", "configuration"});
	RegisterTableFunction(loader, GetGlobCacheConfigQueryFunc(), "Returns the current glob cache configuration.",
	                      {"SELECT * FROM cache_httpfs_get_glob_cache_config();"}, {"cache_httpfs", "configuration"});
	RegisterTableFunction(loader, GetCacheTypeQueryFunc(),
	                      "Returns the active cache type and whether caching is enabled.",
	                      {"SELECT * FROM cache_httpfs_get_cache_type();"}, {"cache_httpfs", "configuration"});
	RegisterTableFunction(loader, GetCacheConfigQueryFunc(), "Returns all current cache_httpfs configuration values.",
	                      {"SELECT * FROM cache_httpfs_get_cache_config();"}, {"cache_httpfs", "configuration"});

	RegisterTableFunction(
	    loader, ListRegisteredFileSystemsQueryFunc(), "Lists filesystem implementations registered with DuckDB.",
	    {"SELECT * FROM cache_httpfs_list_registered_filesystems();"}, {"cache_httpfs", "filesystem"});
	RegisterTableFunction(loader, GetCacheAccessInfoQueryFunc(),
	                      "Returns hit, miss, byte, and latency statistics for each cache entity.",
	                      {"SELECT * FROM cache_httpfs_cache_access_info_query();"}, {"cache_httpfs", "observability"});
	RegisterTableFunction(loader, GetWrappedCacheFileSystemsFunc(),
	                      "Lists filesystem implementations currently wrapped by cache_httpfs.",
	                      {"SELECT * FROM cache_httpfs_get_cache_filesystems();"}, {"cache_httpfs", "filesystem"});
}

} // namespace duckdb

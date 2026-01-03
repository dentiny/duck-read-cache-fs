#define DUCKDB_EXTENSION_MAIN

#include "cache_httpfs_extension.hpp"

#include <algorithm>
#include <csignal>

#include "base_profile_collector.hpp"
#include "cache_exclusion_utils.hpp"
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "cache_status_query_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "duckdb/main/setting_info.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include "extension_config_query_function.hpp"
#include "fake_filesystem.hpp"
#include "filesystem_status_query_function.hpp"
#include "hffs.hpp"
#include "httpfs_extension.hpp"
#include "s3fs.hpp"
#include "temp_profile_collector.hpp"

#include <algorithm>

namespace duckdb {

namespace {

// "httpfs" extension name.
constexpr const char *HTTPFS_EXTENSION = "httpfs";

// Get database instance from expression state.
// Returned instance ownership lies in the given [`state`].
DatabaseInstance &GetDatabaseInstance(ExpressionState &state) {
	auto *executor = state.root.executor;
	auto &client_context = executor->GetContext();
	return *client_context.db.get();
}

// Clear both in-memory and on-disk data block cache.
void ClearAllCache(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto &instance = GetDatabaseInstance(state);
	auto &inst_state = GetInstanceStateOrThrow(instance);

	// Special handle local disk cache clear, since it's possible disk cache reader hasn't been initialized.
	auto local_filesystem = LocalFileSystem::CreateLocal();
	for (const auto &cur_cache_dir : inst_state.config.on_disk_cache_directories) {
		local_filesystem->RemoveDirectory(cur_cache_dir);
		local_filesystem->CreateDirectory(cur_cache_dir);
	}

	// Clear data block cache for all initialized cache readers.
	inst_state.cache_reader_manager.ClearCache();
	D_ASSERT(inst_state.profile_collector != nullptr);
	inst_state.profile_collector->Reset();

	// Clear all non data block cache, including file handle cache, glob cache and metadata cache.
	auto cache_filesystem_instances = inst_state.registry.GetAllCacheFs();
	for (auto *cur_cache_fs : cache_filesystem_instances) {
		cur_cache_fs->ClearCache();
	}

	constexpr bool SUCCESS = true;
	result.Reference(Value(SUCCESS));
}

void ClearCacheForFile(const DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	const string filepath = args.GetValue(/*col_idx=*/0, /*index=*/0).ToString();

	auto &instance = GetDatabaseInstance(state);
	auto &inst_state = GetInstanceStateOrThrow(instance);

	// Clear data block cache on the given [fname] for all initialized filesystems.
	inst_state.cache_reader_manager.ClearCache(filepath);

	// Clear all non data block cache, including file handle cache, glob cache and metadata cache.
	auto cache_filesystem_instances = inst_state.registry.GetAllCacheFs();
	for (auto *cur_cache_fs : cache_filesystem_instances) {
		cur_cache_fs->ClearCache(filepath);
	}

	constexpr bool SUCCESS = true;
	result.Reference(Value(SUCCESS));
}

// Get on-disk data cache file size for all cache filesystems.
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

	string latest_stat;
	uint64_t latest_timestamp = 0;
	const auto cache_file_systems = inst_state.registry.GetAllCacheFs();
	for (auto *cur_filesystem : cache_file_systems) {
		auto &profile_collector = cur_filesystem->GetProfileCollector();
		auto stats_pair = profile_collector.GetHumanReadableStats();
		const auto &cur_profile_stat = stats_pair.first;
		const auto &cur_timestamp = stats_pair.second;
		// Skip collectors that have never been accessed.
		if (cur_timestamp == 0) {
			continue;
		}
		if (cur_timestamp > latest_timestamp) {
			latest_timestamp = cur_timestamp;
			latest_stat = std::move(stats_pair.first);
			continue;
		}
		if (cur_timestamp == latest_timestamp) {
			latest_stat = MaxValue<string>(latest_stat, cur_profile_stat);
		}
	}

	if (latest_stat.empty()) {
		latest_stat = "No valid access to cache filesystem";
	}
	result.Reference(Value(std::move(latest_stat)));
}

void ResetProfileStats(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto &instance = GetDatabaseInstance(state);
	auto &inst_state = GetInstanceStateOrThrow(instance);

	const auto cache_file_systems = inst_state.registry.GetAllCacheFs();
	for (auto *cur_filesystem : cache_file_systems) {
		auto &profile_collector = cur_filesystem->GetProfileCollector();
		profile_collector.Reset();
	}

	constexpr bool SUCCESS = true;
	result.Reference(Value(SUCCESS));
}

// Wrap the filesystem with extension cache filesystem.
// Throw exception if the requested filesystem hasn't been registered into duckdb instance.
void WrapCacheFileSystem(const DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	const string filesystem_name = args.GetValue(/*col_idx=*/0, /*index=*/0).ToString();

	// duckdb instance has a opener filesystem, which is a wrapper around virtual filesystem.
	auto &duckdb_instance = GetDatabaseInstance(state);
	auto &opener_filesystem = duckdb_instance.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	auto internal_filesystem = vfs.ExtractSubSystem(filesystem_name);
	if (internal_filesystem == nullptr) {
		throw InvalidInputException("Filesystem %s hasn't been registered yet!", filesystem_name);
	}

	auto cache_filesystem =
	    make_uniq<CacheFileSystem>(std::move(internal_filesystem), GetInstanceStateShared(duckdb_instance));
	vfs.RegisterSubSystem(std::move(cache_filesystem));
	DUCKDB_LOG_DEBUG(duckdb_instance, StringUtil::Format("Wrap filesystem %s with cache filesystem.", filesystem_name));

	constexpr bool SUCCESS = true;
	result.Reference(Value(SUCCESS));
}

// Extract or get httpfs filesystem.
unique_ptr<FileSystem> ExtractOrCreateHttpfs(FileSystem &vfs) {
	auto filesystems = vfs.ListSubSystems();
	auto iter = std::find_if(filesystems.begin(), filesystems.end(), [](const auto &cur_fs_name) {
		// Wrapped filesystem made by extensions could ends with httpfs filesystem.
		return StringUtil::EndsWith(cur_fs_name, "HTTPFileSystem");
	});
	if (iter == filesystems.end()) {
		return make_uniq<HTTPFileSystem>();
	}
	auto httpfs = vfs.ExtractSubSystem(*iter);
	D_ASSERT(httpfs != nullptr);
	return httpfs;
}

// Extract or get hugging filesystem.
unique_ptr<FileSystem> ExtractOrCreateHuggingfs(FileSystem &vfs) {
	auto filesystems = vfs.ListSubSystems();
	auto iter = std::find_if(filesystems.begin(), filesystems.end(), [](const auto &cur_fs_name) {
		// Wrapped filesystem made by extensions could ends with httpfs filesystem.
		return StringUtil::EndsWith(cur_fs_name, "HuggingFaceFileSystem");
	});
	if (iter == filesystems.end()) {
		return make_uniq<HuggingFaceFileSystem>();
	}
	auto hf_fs = vfs.ExtractSubSystem(*iter);
	D_ASSERT(hf_fs != nullptr);
	return hf_fs;
}

// Extract or get s3 filesystem.
unique_ptr<FileSystem> ExtractOrCreateS3fs(FileSystem &vfs, DatabaseInstance &instance) {
	auto filesystems = vfs.ListSubSystems();
	auto iter = std::find_if(filesystems.begin(), filesystems.end(), [](const auto &cur_fs_name) {
		// Wrapped filesystem made by extensions could ends with s3 filesystem.
		return StringUtil::EndsWith(cur_fs_name, "S3FileSystem");
	});
	if (iter == filesystems.end()) {
		return make_uniq<S3FileSystem>(BufferManager::GetBufferManager(instance));
	}
	auto s3_fs = vfs.ExtractSubSystem(*iter);
	D_ASSERT(s3_fs != nullptr);
	return s3_fs;
}

// Whether `httpfs` extension has already been loaded.
bool IsHttpfsExtensionLoaded(DatabaseInstance &db_instance) {
	auto &extension_manager = db_instance.GetExtensionManager();
	const auto loaded_extensions = extension_manager.GetExtensions();
	return std::find(loaded_extensions.begin(), loaded_extensions.end(), HTTPFS_EXTENSION) != loaded_extensions.end();
}

//===--------------------------------------------------------------------===//
// Extension option callbacks - update instance state config when settings change
//===--------------------------------------------------------------------===//

void SetCacheType(DatabaseInstance &duckdb_instance, string cache_type_str) {
	if (ALL_CACHE_TYPES->find(cache_type_str) == ALL_CACHE_TYPES->end()) {
		vector<string> valid_types(ALL_CACHE_TYPES->begin(), ALL_CACHE_TYPES->end());
		throw InvalidInputException("Invalid cache_httpfs_type '%s'. Valid options are: %s", cache_type_str,
		                            StringUtil::Join(valid_types, ", "));
	}
	auto &instance_state = GetInstanceStateOrThrow(duckdb_instance);
	instance_state.config.cache_type = std::move(cache_type_str);
	auto state = GetInstanceStateShared(duckdb_instance);
	instance_state.cache_reader_manager.SetCacheReader(instance_state.config, state);
}

void UpdateCacheType(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	auto cache_type_str = parameter.ToString();
	SetCacheType(*context.db, std::move(cache_type_str));
}

void UpdateCacheBlockSize(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto cache_block_size = parameter.GetValue<uint64_t>();
	if (cache_block_size == 0) {
		throw InvalidInputException("cache_httpfs_cache_block_size must be greater than 0");
	}
	inst_state.config.cache_block_size = cache_block_size;
}

void UpdateProfileType(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	auto profile_type_str = parameter.ToString();
	if (ALL_PROFILE_TYPES->find(profile_type_str) == ALL_PROFILE_TYPES->end()) {
		const vector<string> valid_types(ALL_PROFILE_TYPES->begin(), ALL_PROFILE_TYPES->end());
		throw InvalidInputException("Invalid cache_httpfs_profile_type '%s'. Valid options are: %s", profile_type_str,
		                            StringUtil::Join(valid_types, ", "));
	}
	inst_state.config.profile_type = std::move(profile_type_str);

	// Initialize the profile collector based on the new profile type
	SetProfileCollector(inst_state, inst_state.config.profile_type);
	// Update all cache readers to use the new collector
	inst_state.cache_reader_manager.UpdateProfileCollector(*inst_state.profile_collector);
	// Update all registered cache filesystems to use the new profile collector
	auto cache_filesystems = inst_state.registry.GetAllCacheFs();
	for (auto *cache_fs : cache_filesystems) {
		cache_fs->SetProfileCollector(*inst_state.profile_collector);
	}
}

void UpdateMaxFanoutSubrequest(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto max_subrequest_count = parameter.GetValue<uint64_t>();
	if (max_subrequest_count == 0) {
		throw InvalidInputException("cache_httpfs_max_fanout_subrequest must be greater than 0");
	}
	inst_state.config.max_subrequest_count = max_subrequest_count;
}

void UpdateEnableCacheValidation(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	inst_state.config.enable_cache_validation = parameter.GetValue<bool>();
}

// Implementation for check cache directories and create directories if necessary.
void UpdateCacheDirectoriesImpl(CacheHttpfsInstanceState &inst_state, vector<string> directories) {
	std::sort(directories.begin(), directories.end());
	if (directories == inst_state.config.on_disk_cache_directories) {
		return;
	}

	auto local_fs = LocalFileSystem::CreateLocal();
	for (const auto &dir : directories) {
		local_fs->CreateDirectory(dir);
	}
	inst_state.config.on_disk_cache_directories = std::move(directories);
}

void UpdateCacheDirectory(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	auto new_cache_directory = parameter.ToString();

	// If empty directory is provided, fall back to default directory.
	if (new_cache_directory.empty()) {
		new_cache_directory = GetDefaultOnDiskCacheDirectory();
	}

	vector<string> directories;
	directories.emplace_back(std::move(new_cache_directory));
	UpdateCacheDirectoriesImpl(inst_state, std::move(directories));
}

void UpdateCacheDirectoriesConfig(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	auto directories_config_str = parameter.ToString();

	vector<string> directories;
	if (!directories_config_str.empty()) {
		directories = StringUtil::Split(directories_config_str, ';');
	}
	// If the provided config is set to empty, fall back to default directory.
	else {
		directories.emplace_back(GetDefaultOnDiskCacheDirectory());
	}

	UpdateCacheDirectoriesImpl(inst_state, std::move(directories));
}

void UpdateMinDiskBytesForCache(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto disk_cache_min_bytes = parameter.GetValue<uint64_t>();
	if (disk_cache_min_bytes == 0) {
		throw InvalidInputException("cache_httpfs_min_disk_bytes_for_cache must be greater than 0");
	}
	inst_state.config.min_disk_bytes_for_cache = disk_cache_min_bytes;
}

void UpdateEvictPolicy(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto eviction_policy = parameter.ToString();
	if (eviction_policy == *ON_DISK_CREATION_TIMESTAMP_EVICTION) {
		inst_state.config.on_disk_eviction_policy = *ON_DISK_CREATION_TIMESTAMP_EVICTION;
	} else if (eviction_policy == *ON_DISK_LRU_SINGLE_PROC_EVICTION) {
		inst_state.config.on_disk_eviction_policy = *ON_DISK_LRU_SINGLE_PROC_EVICTION;
	} else {
		throw InvalidInputException("Invalid cache_httpfs_evict_policy '%s'. Valid options are: %s, %s",
		                            eviction_policy, *ON_DISK_CREATION_TIMESTAMP_EVICTION,
		                            *ON_DISK_LRU_SINGLE_PROC_EVICTION);
	}
}

void UpdateDiskCacheReaderEnableMemoryCache(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	inst_state.config.enable_disk_reader_mem_cache = parameter.GetValue<bool>();
}

void UpdateDiskCacheReaderMemCacheBlockCount(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto block_count = parameter.GetValue<idx_t>();
	if (block_count == 0) {
		throw InvalidInputException("cache_httpfs_disk_cache_reader_mem_cache_block_count must be greater than 0");
	}
	inst_state.config.disk_reader_max_mem_cache_block_count = block_count;
}

void UpdateDiskCacheReaderMemCacheTimeout(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto timeout = parameter.GetValue<idx_t>();
	if (timeout == 0) {
		throw InvalidInputException("cache_httpfs_disk_cache_reader_mem_cache_timeout_millisec must be greater than 0");
	}
	inst_state.config.disk_reader_max_mem_cache_timeout_millisec = timeout;
}

void UpdateMaxInMemCacheBlockCount(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto in_mem_block_count = parameter.GetValue<uint64_t>();
	if (in_mem_block_count == 0) {
		throw InvalidInputException("cache_httpfs_max_in_mem_cache_block_count must be greater than 0");
	}
	inst_state.config.max_in_mem_cache_block_count = in_mem_block_count;
}

void UpdateInMemCacheBlockTimeout(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto timeout = parameter.GetValue<uint64_t>();
	if (timeout == 0) {
		throw InvalidInputException("cache_httpfs_in_mem_cache_block_timeout_millisec must be greater than 0");
	}
	inst_state.config.in_mem_cache_block_timeout_millisec = timeout;
}

void UpdateEnableMetadataCache(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	inst_state.config.enable_metadata_cache = parameter.GetValue<bool>();
}

void UpdateMetadataCacheEntrySize(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto entry_size = parameter.GetValue<uint64_t>();
	if (entry_size == 0) {
		throw InvalidInputException("cache_httpfs_metadata_cache_entry_size must be greater than 0");
	}
	inst_state.config.max_metadata_cache_entry = entry_size;
}

void UpdateMetadataCacheEntryTimeout(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto timeout = parameter.GetValue<uint64_t>();
	if (timeout == 0) {
		throw InvalidInputException("cache_httpfs_metadata_cache_entry_timeout_millisec must be greater than 0");
	}
	inst_state.config.metadata_cache_entry_timeout_millisec = timeout;
}

void UpdateEnableFileHandleCache(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	inst_state.config.enable_file_handle_cache = parameter.GetValue<bool>();
}

void UpdateFileHandleCacheEntrySize(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto entry_size = parameter.GetValue<uint64_t>();
	if (entry_size == 0) {
		throw InvalidInputException("cache_httpfs_file_handle_cache_entry_size must be greater than 0");
	}
	inst_state.config.max_file_handle_cache_entry = entry_size;
}

void UpdateFileHandleCacheEntryTimeout(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto timeout = parameter.GetValue<uint64_t>();
	if (timeout == 0) {
		throw InvalidInputException("cache_httpfs_file_handle_cache_entry_timeout_millisec must be greater than 0");
	}
	inst_state.config.file_handle_cache_entry_timeout_millisec = timeout;
}

void UpdateEnableGlobCache(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	inst_state.config.enable_glob_cache = parameter.GetValue<bool>();
}

void UpdateGlobCacheEntrySize(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto entry_size = parameter.GetValue<uint64_t>();
	if (entry_size == 0) {
		throw InvalidInputException("cache_httpfs_glob_cache_entry_size must be greater than 0");
	}
	inst_state.config.max_glob_cache_entry = entry_size;
}

void UpdateGlobCacheEntryTimeout(ClientContext &context, SetScope scope, Value &parameter) {
	auto &inst_state = GetInstanceStateOrThrow(context);
	const auto timeout = parameter.GetValue<uint64_t>();
	if (timeout == 0) {
		throw InvalidInputException("cache_httpfs_glob_cache_entry_timeout_millisec must be greater than 0");
	}
	inst_state.config.glob_cache_entry_timeout_millisec = timeout;
}

// Cached httpfs cannot co-exist with non-cached version, because duckdb virtual filesystem doesn't provide a native fs
// wrapper nor priority system, so co-existence doesn't guarantee cached version is actually used.
//
// Here's how we handled (a hacky way):
// 1. When we register cached filesystem, if uncached version already registered, we unregister them.
// 2. If uncached filesystem is registered later somehow, cached version is set mutual set so it has higher priority
// than uncached version.
void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();

	// Create per-instance state for this extension
	auto state = make_shared_ptr<CacheHttpfsInstanceState>();

	// Initialize profile collector based on default profile type
	SetProfileCollector(*state, state->config.profile_type);

	SetInstanceState(instance, state);

	// Ensure cache directory exists
	auto local_fs = LocalFileSystem::CreateLocal();
	for (const auto &dir : state->config.on_disk_cache_directories) {
		local_fs->CreateDirectory(dir);
	}

	// When cache httpfs enabled, by default disable external file cache, otherwise double buffering.
	// Users could re-enable by setting the config afterwards.
	instance.config.options.enable_external_file_cache = false;
	instance.GetExternalFileCache().SetEnabled(false);

	// To achieve full compatibility for duckdb-httpfs extension, all related functions/types/... should be supported,
	// so we load it first if not already loaded.
	const bool httpfs_extension_loaded = IsHttpfsExtensionLoaded(instance);
	if (!httpfs_extension_loaded) {
		auto httpfs_extension = make_uniq<HttpfsExtension>();
		httpfs_extension->Load(loader);

		// Register into extension manager to keep compatibility as httpfs.
		auto &extension_manager = ExtensionManager::Get(instance);
		auto extension_active_load = extension_manager.BeginLoad(HTTPFS_EXTENSION);
		// Manually fill in the extension install info to finalize extension load.
		ExtensionInstallInfo extension_install_info;
		extension_install_info.mode = ExtensionInstallMode::UNKNOWN;
		extension_active_load->FinishLoad(extension_install_info);
	}

	// Register filesystem instance to instance.
	// Here we register both in-memory filesystem and on-disk filesystem, and leverage global configuration to decide
	// which one to use.
	auto &opener_filesystem = instance.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();

	// TODO(hjiang): Register a fake filesystem at extension load for testing purpose. This is not ideal since
	// additional necessary instance is shipped in the extension. Local filesystem is not viable because it's not
	// registered in virtual filesystem. A better approach is find another filesystem not in httpfs extension.
	vfs.RegisterSubSystem(make_uniq<CacheHttpfsFakeFileSystem>());

	// By default register all filesystem instances inside of httpfs.
	// CacheFileSystem constructor auto-registers with instance registry.
	//
	// Register http filesystem.
	auto http_fs = ExtractOrCreateHttpfs(vfs);
	auto cache_httpfs_filesystem = make_uniq<CacheFileSystem>(std::move(http_fs), state);
	vfs.RegisterSubSystem(std::move(cache_httpfs_filesystem));
	DUCKDB_LOG_DEBUG(instance, "Wrap HTTPFileSystem with cache filesystem.");

	// Register hugging filesystem.
	auto hf_fs = ExtractOrCreateHuggingfs(vfs);
	auto cache_hf_filesystem = make_uniq<CacheFileSystem>(std::move(hf_fs), state);
	vfs.RegisterSubSystem(std::move(cache_hf_filesystem));
	DUCKDB_LOG_DEBUG(instance, "Wrap HuggingFaceFileSystem with cache filesystem.");

	// Register s3 filesystem.
	auto s3_fs = ExtractOrCreateS3fs(vfs, instance);
	auto cache_s3_filesystem = make_uniq<CacheFileSystem>(std::move(s3_fs), state);
	vfs.RegisterSubSystem(std::move(cache_s3_filesystem));
	DUCKDB_LOG_DEBUG(instance, "Wrap S3FileSystem with cache filesystem.");

	// Set initial cache reader.
	SetCacheType(instance, *DEFAULT_CACHE_TYPE);

	// Register extension configuration.
	auto &config = DBConfig::GetConfig(instance);

	// Global configurations.
	config.AddExtensionOption("cache_httpfs_type",
	                          "Type for cached filesystem. Currently there're two types available, one is `in_mem`, "
	                          "another is `on_disk`. By default we use on-disk cache. Set to `noop` to disable, which "
	                          "behaves exactly same as httpfs extension.",
	                          LogicalType {LogicalTypeId::VARCHAR}, *ON_DISK_CACHE_TYPE, UpdateCacheType);
	config.AddExtensionOption(
	    "cache_httpfs_cache_block_size",
	    "Block size for cache, applies to both in-memory cache filesystem and on-disk cache filesystem. It's worth "
	    "noting for on-disk filesystem, all existing cache files are invalidated after config update.",
	    LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_CACHE_BLOCK_SIZE), UpdateCacheBlockSize);
	config.AddExtensionOption(
	    "cache_httpfs_profile_type",
	    "Profiling type for cached filesystem. There're three options available: `noop`, `temp`, and `duckdb`. `temp` "
	    "option stores the latest IO operation profiling result, which potentially suffers concurrent updates; "
	    "`duckdb` stores the IO operation profiling results into duckdb table, which unblocks advanced analysis.",
	    LogicalType {LogicalTypeId::VARCHAR}, *DEFAULT_PROFILE_TYPE, UpdateProfileType);
	config.AddExtensionOption(
	    "cache_httpfs_max_fanout_subrequest",
	    "Cached httpfs performs parallel request by splittng them into small request, with request size decided by "
	    "config [cache_httpfs_cache_block_size]. The setting limits the maximum request to issue for a single "
	    "filesystem read request. 0 means no limit, by default we set no limit.",
	    LogicalType {LogicalTypeId::BIGINT}, 0, UpdateMaxFanoutSubrequest);

	// Add configurations to ignore SIGPIPE.
	// Notice, it only works on unix platform.
	auto ignore_sigpipe_callback = [](ClientContext &context, SetScope scope, Value &parameter) {
#if !defined(_WIN32)
		const bool ignore = parameter.GetValue<bool>();
		if (ignore) {
			// Ignore SIGPIPE, reference: https://blog.erratasec.com/2018/10/tcpip-sockets-and-sigpipe.html
			std::signal(SIGPIPE, SIG_IGN);
		}
#endif
	};
	config.AddExtensionOption(
	    "cache_httpfs_ignore_sigpipe",
	    "Whether to ignore SIGPIPE for the extension. By default not ignored. Once ignored, it cannot be reverted.",
	    LogicalTypeId::BOOLEAN, DEFAULT_IGNORE_SIGPIPE, std::move(ignore_sigpipe_callback));

	// Cache validation config.
	config.AddExtensionOption("cache_httpfs_enable_cache_validation",
	                          "Whether to enable cache validation using version tag and last modification timestamp. "
	                          "When enabled, cache entries are validated against the current file version tag and "
	                          "modification timestamp to ensure cache consistency. By default disabled.",
	                          LogicalTypeId::BOOLEAN, DEFAULT_ENABLE_CACHE_VALIDATION, UpdateEnableCacheValidation);

	// On disk cache config.
	// TODO(hjiang): Add a new configurable for on-disk cache staleness.
	config.AddExtensionOption("cache_httpfs_cache_directory", "The disk cache directory that stores cached data",
	                          LogicalType {LogicalTypeId::VARCHAR}, GetDefaultOnDiskCacheDirectory(),
	                          UpdateCacheDirectory);
	config.AddExtensionOption("cache_httpfs_min_disk_bytes_for_cache",
	                          "Min number of bytes on disk for the cache filesystem to enable on-disk cache; if left "
	                          "bytes is less than the threshold, LRU based cache file eviction will be performed."
	                          "By default, 5% disk space will be reserved for other usage. When min disk bytes "
	                          "specified with a positive value, the default value will be overriden.",
	                          LogicalType {LogicalTypeId::UBIGINT}, 0, UpdateMinDiskBytesForCache);
	config.AddExtensionOption(
	    "cache_httpfs_evict_policy",
	    "Eviction policy for on-disk cache cache blocks. By default "
	    "it's creation timestamp based ('creation_timestamp'), which deletes all cache blocks "
	    "created earlier than threshold. Other supported policy include 'lru_single_proc' (LRU for"
	    "single process access), which performs LRU-based eviction, mainly made single process"
	    "usage.",
	    LogicalType {LogicalTypeId::VARCHAR}, *DEFAULT_ON_DISK_EVICTION_POLICY, UpdateEvictPolicy);
	// TODO(hjiang): there're quite a few optimizations which could be done in the config. For example,
	// - Each cache directories could have their own config, like min/max cache file size;
	// - Current implementation uses static hash based distribution, which doesn't work well when directory set changes;
	// there're a few ways to resolve this problem, for example, fallback to other cache directories and check; change
	// distribution logic.
	config.AddExtensionOption(
	    "cache_httpfs_cache_directories_config",
	    "Advanced configuration for on-disk cache. It supports multiple directories, separated by semicolons (';'). "
	    "Cache blocks will be evenly distributed under different directories deterministically."
	    "Between different runs, it's expected to provide same cache directories, otherwise it's not guaranteed cache "
	    "files still exist and accessible."
	    "Overrides 'cache_httpfs_cache_directory' if set.",
	    LogicalType {LogicalTypeId::VARCHAR}, string {}, UpdateCacheDirectoriesConfig);

	// Memory cache for disk cache reader.
	config.AddExtensionOption("cache_httpfs_disk_cache_reader_enable_memory_cache",
	                          "Whether enable process-wise read-through/write-through cache for disk cache reader. "
	                          "When enabled, local cache file will be accessed with direct IO.",
	                          LogicalTypeId::BOOLEAN, DEFAULT_ENABLE_DISK_READER_MEM_CACHE,
	                          UpdateDiskCacheReaderEnableMemoryCache);
	config.AddExtensionOption(
	    "cache_httpfs_disk_cache_reader_mem_cache_block_count",
	    "Max number of cache blocks for the read-through/write-through cache for disk cache reader.",
	    LogicalTypeId::UBIGINT, Value::UBIGINT(DEFAULT_MAX_DISK_READER_MEM_CACHE_BLOCK_COUNT),
	    UpdateDiskCacheReaderMemCacheBlockCount);
	config.AddExtensionOption("cache_httpfs_disk_cache_reader_mem_cache_timeout_millisec",
	                          "Timeout in milliseconds for the read-through/write-through cache for disk cache reader.",
	                          LogicalTypeId::UBIGINT, Value::UBIGINT(DEFAULT_DISK_READER_MEM_CACHE_TIMEOUT_MILLISEC),
	                          UpdateDiskCacheReaderMemCacheTimeout);

	// In-memory cache config.
	config.AddExtensionOption("cache_httpfs_max_in_mem_cache_block_count",
	                          "Max in-memory cache block count for in-memory caches for all cache filesystems, so "
	                          "users are able to configure the maximum memory consumption. It's worth noting it "
	                          "should be set only once before all filesystem access, otherwise there's no affect.",
	                          LogicalType {LogicalTypeId::UBIGINT},
	                          Value::UBIGINT(DEFAULT_MAX_IN_MEM_CACHE_BLOCK_COUNT), UpdateMaxInMemCacheBlockCount);
	config.AddExtensionOption("cache_httpfs_in_mem_cache_block_timeout_millisec",
	                          "Data block cache entry timeout in milliseconds.", LogicalTypeId::UBIGINT,
	                          Value::UBIGINT(DEFAULT_IN_MEM_BLOCK_CACHE_TIMEOUT_MILLISEC),
	                          UpdateInMemCacheBlockTimeout);

	// Metadata cache config.
	config.AddExtensionOption("cache_httpfs_enable_metadata_cache",
	                          "Whether metadata cache is enable for cache filesystem. By default enabled.",
	                          LogicalTypeId::BOOLEAN, DEFAULT_ENABLE_METADATA_CACHE, UpdateEnableMetadataCache);
	config.AddExtensionOption("cache_httpfs_metadata_cache_entry_size", "Max cache size for metadata LRU cache.",
	                          LogicalTypeId::UBIGINT, Value::UBIGINT(DEFAULT_MAX_METADATA_CACHE_ENTRY),
	                          UpdateMetadataCacheEntrySize);
	config.AddExtensionOption("cache_httpfs_metadata_cache_entry_timeout_millisec",
	                          "Cache entry timeout in milliseconds for metadata LRU cache.", LogicalTypeId::UBIGINT,
	                          Value::UBIGINT(DEFAULT_METADATA_CACHE_ENTRY_TIMEOUT_MILLISEC),
	                          UpdateMetadataCacheEntryTimeout);

	// File handle cache config.
	config.AddExtensionOption("cache_httpfs_enable_file_handle_cache",
	                          "Whether file handle cache is enable for cache filesystem. By default enabled.",
	                          LogicalTypeId::BOOLEAN, DEFAULT_ENABLE_FILE_HANDLE_CACHE, UpdateEnableFileHandleCache);
	config.AddExtensionOption("cache_httpfs_file_handle_cache_entry_size", "Max cache size for file handle cache.",
	                          LogicalTypeId::UBIGINT, Value::UBIGINT(DEFAULT_MAX_FILE_HANDLE_CACHE_ENTRY),
	                          UpdateFileHandleCacheEntrySize);
	config.AddExtensionOption("cache_httpfs_file_handle_cache_entry_timeout_millisec",
	                          "Cache entry timeout in milliseconds for file handle cache.", LogicalTypeId::UBIGINT,
	                          Value::UBIGINT(DEFAULT_FILE_HANDLE_CACHE_ENTRY_TIMEOUT_MILLISEC),
	                          UpdateFileHandleCacheEntryTimeout);

	// Glob cache config.
	config.AddExtensionOption("cache_httpfs_enable_glob_cache",
	                          "Whether glob cache is enable for cache filesystem. By default enabled.",
	                          LogicalTypeId::BOOLEAN, DEFAULT_ENABLE_GLOB_CACHE, UpdateEnableGlobCache);
	config.AddExtensionOption("cache_httpfs_glob_cache_entry_size", "Max cache size for glob cache.",
	                          LogicalTypeId::UBIGINT, Value::UBIGINT(DEFAULT_MAX_GLOB_CACHE_ENTRY),
	                          UpdateGlobCacheEntrySize);
	config.AddExtensionOption("cache_httpfs_glob_cache_entry_timeout_millisec",
	                          "Cache entry timeout in milliseconds for glob cache.", LogicalTypeId::UBIGINT,
	                          Value::UBIGINT(DEFAULT_GLOB_CACHE_ENTRY_TIMEOUT_MILLISEC), UpdateGlobCacheEntryTimeout);

	// Cache exclusion regex list.
	ScalarFunction add_cache_exclusion_regex("cache_httpfs_add_exclusion_regex",
	                                         /*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}},
	                                         /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN},
	                                         AddCacheExclusionRegex);
	loader.RegisterFunction(add_cache_exclusion_regex);

	ScalarFunction reset_cache_exclusion_regex("cache_httpfs_reset_exclusion_regex",
	                                           /*arguments=*/ {},
	                                           /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN},
	                                           ResetCacheExclusionRegex);
	loader.RegisterFunction(reset_cache_exclusion_regex);

	loader.RegisterFunction(ListCacheExclusionRegex());

	// Register cache cleanup function for data cache (both in-memory and on-disk cache) and other types of cache.
	ScalarFunction clear_cache_function("cache_httpfs_clear_cache", /*arguments=*/ {},
	                                    /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, ClearAllCache);
	loader.RegisterFunction(clear_cache_function);

	// Register cache cleanup function for the given filename.
	ScalarFunction clear_cache_for_file_function("cache_httpfs_clear_cache_for_file",
	                                             /*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}},
	                                             /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN},
	                                             ClearCacheForFile);
	loader.RegisterFunction(clear_cache_for_file_function);

	// Register a function to wrap all duckdb-vfs-compatible filesystems. By default only httpfs filesystem instances
	// are wrapped. Usage for the target filesystem can be used as normal.
	//
	// Example usage:
	// D. LOAD azure;
	// -- Wrap filesystem with its name.
	// D. SELECT cache_httpfs_wrap_cache_filesystem('AzureBlobStorageFileSystem');
	ScalarFunction wrap_cache_filesystem_function("cache_httpfs_wrap_cache_filesystem",
	                                              /*arguments=*/ {LogicalTypeId::VARCHAR},
	                                              /*return_type=*/LogicalTypeId::BOOLEAN, WrapCacheFileSystem);
	loader.RegisterFunction(wrap_cache_filesystem_function);

	// Register on-disk data cache file size stat function.
	ScalarFunction get_ondisk_data_cache_size_function("cache_httpfs_get_ondisk_data_cache_size", /*arguments=*/ {},
	                                                   /*return_type=*/LogicalType {LogicalTypeId::BIGINT},
	                                                   GetOnDiskDataCacheSize);
	loader.RegisterFunction(get_ondisk_data_cache_size_function);

	// Register on-disk cache file display.
	loader.RegisterFunction(GetDataCacheStatusQueryFunc());

	// Register profile collector metrics.
	// A commonly-used SQL is `COPY (SELECT cache_httpfs_get_profile()) TO '/tmp/output.txt';`.
	ScalarFunction get_profile_stats_function("cache_httpfs_get_profile", /*arguments=*/ {},
	                                          /*return_type=*/LogicalType {LogicalTypeId::VARCHAR}, GetProfileStats);
	loader.RegisterFunction(get_profile_stats_function);

	// Register profile collector metrics reset.
	ScalarFunction clear_profile_stats_function("cache_httpfs_clear_profile", /*arguments=*/ {},
	                                            /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN},
	                                            ResetProfileStats);
	loader.RegisterFunction(clear_profile_stats_function);

	// Register table function to get current cache config.
	loader.RegisterFunction(GetDataCacheConfigQueryFunc());
	loader.RegisterFunction(GetMetadataCacheConfigQueryFunc());
	loader.RegisterFunction(GetFileHandleCacheConfigQueryFunc());
	loader.RegisterFunction(GetGlobCacheConfigQueryFunc());
	loader.RegisterFunction(GetCacheTypeQueryFunc());
	loader.RegisterFunction(GetCacheConfigQueryFunc());

	// Register filesystem registration query function.
	loader.RegisterFunction(ListRegisteredFileSystemsQueryFunc());

	// Register cache access metrics.
	loader.RegisterFunction(GetCacheAccessInfoQueryFunc());

	// Create default cache directory.
	LocalFileSystem::CreateLocal()->CreateDirectory(GetDefaultOnDiskCacheDirectory());

	// Register wrapped cache filesystems info.
	loader.RegisterFunction(GetWrappedCacheFileSystemsFunc());

	// Fill in extension load information.
	string description = StringUtil::Format(
	    "Adds a read cache filesystem to DuckDB, which acts as a wrapper of duckdb-compatible filesystems.");
	loader.SetDescription(description);
}

} // namespace

void CacheHttpfsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
string CacheHttpfsExtension::Name() {
	return "cache_httpfs";
}

string CacheHttpfsExtension::Version() const {
#ifdef EXT_VERSION_CACHE_HTTPFS
	return EXT_VERSION_CACHE_HTTPFS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(cache_httpfs, loader) {
	duckdb::CacheHttpfsExtension().Load(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

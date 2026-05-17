#include "in_memory_data_cache_storage.hpp"

#include "cache_filesystem_config.hpp"
#include "duckdb/common/exception.hpp"
#include "extension_bounded_data_cache_storage.hpp"
#include "object_cache_data_cache_storage.hpp"

namespace duckdb {

shared_ptr<InMemoryDataCacheStorage> BuildInMemoryDataCacheStorage(const string &mode,
                                                                   optional_ptr<DatabaseInstance> db_instance,
                                                                   size_t max_entries, uint64_t timeout_millisec) {
	if (mode == *OBJECT_CACHE_STORAGE) {
		ALWAYS_ASSERT(db_instance != nullptr);
		return make_shared_ptr<ObjectCacheStorage>(*db_instance, timeout_millisec);
	}
	return make_shared_ptr<ExtensionBoundedDataCacheStorage>(max_entries, timeout_millisec);
}

} // namespace duckdb

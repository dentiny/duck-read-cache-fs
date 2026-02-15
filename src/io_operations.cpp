#include "io_operations.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Cache entity name, indexed by cache entity enum.
const vector<const char *> CACHE_ENTITY_NAMES = []() {
	vector<const char *> cache_entity_names;
	cache_entity_names.reserve(kCacheEntityCount);
	cache_entity_names.emplace_back("metadata");
	cache_entity_names.emplace_back("data");
	cache_entity_names.emplace_back("file handle");
	cache_entity_names.emplace_back("glob");
	D_ASSERT(cache_entity_names.size() == kCacheEntityCount);
	return cache_entity_names;
}();

// Operation names, indexed by operation enums.
const vector<const char *> OPER_NAMES = []() {
	vector<const char *> oper_names;
	oper_names.reserve(kIoOperationCount);
	oper_names.emplace_back("open");
	oper_names.emplace_back("read");
	oper_names.emplace_back("write");
	oper_names.emplace_back("file_sync");
	oper_names.emplace_back("file_remove");
	oper_names.emplace_back("glob");
	oper_names.emplace_back("disk_cache_read");
	oper_names.emplace_back("file_path_cache_clear");
	D_ASSERT(oper_names.size() == kIoOperationCount);
	return oper_names;
}();

} // namespace duckdb

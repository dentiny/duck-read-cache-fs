#include "test_utils.hpp"

#include "cache_filesystem_config.hpp"
#include "cache_reader_manager.hpp"

namespace duckdb {

void ResetGlobalStateAndConfig() {
	ResetGlobalConfig();
	CacheReaderManager::Get().Reset();
}

} // namespace duckdb

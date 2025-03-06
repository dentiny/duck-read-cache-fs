#include "cache_filesystem_ref_registry.hpp"

#include "cache_filesystem.hpp"

namespace duckdb {

/*static*/ CacheFsRefRegistry &CacheFsRefRegistry::Get() {
	static CacheFsRefRegistry registry;
	return registry;
}

void CacheFsRefRegistry::Register(CacheFileSystem *fs) {
	cache_filesystems.emplace_back(fs);
}

void CacheFsRefRegistry::Reset() {
	cache_filesystems.clear();
}

const vector<CacheFileSystem *> &CacheFsRefRegistry::GetAllCacheFs() const {
	return cache_filesystems;
}

} // namespace duckdb

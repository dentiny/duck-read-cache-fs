#include "base_cache_reader.hpp"
#include "cache_reader_manager.hpp"
#include "disk_cache_reader.hpp"
#include "in_memory_cache_reader.hpp"
#include "noop_cache_reader.hpp"

namespace duckdb {

/*static*/ CacheReaderManager &CacheReaderManager::Get() {
	static CacheReaderManager cache_reader_manager {};
	return cache_reader_manager;
}

void CacheReaderManager::InitializeDiskCacheReader() {
	if (on_disk_cache_reader == nullptr) {
		on_disk_cache_reader = make_uniq<DiskCacheReader>();
	}
}

void CacheReaderManager::SetCacheReader() {
	if (*g_cache_type == *NOOP_CACHE_TYPE) {
		if (noop_cache_reader == nullptr) {
			noop_cache_reader = make_uniq<NoopCacheReader>();
		}
		internal_cache_reader = noop_cache_reader.get();
		return;
	}

	if (*g_cache_type == *ON_DISK_CACHE_TYPE) {
		if (on_disk_cache_reader == nullptr) {
			on_disk_cache_reader = make_uniq<DiskCacheReader>();
		}
		internal_cache_reader = on_disk_cache_reader.get();
		return;
	}

	if (*g_cache_type == *IN_MEM_CACHE_TYPE) {
		if (in_mem_cache_reader == nullptr) {
			in_mem_cache_reader = make_uniq<InMemoryCacheReader>();
		}
		internal_cache_reader = in_mem_cache_reader.get();
		return;
	}
}

BaseCacheReader *CacheReaderManager::GetCacheReader() const {
	return internal_cache_reader;
}

vector<BaseCacheReader *> CacheReaderManager::GetCacheReaders() const {
	vector<BaseCacheReader *> cache_readers;
	if (in_mem_cache_reader != nullptr) {
		cache_readers.emplace_back(in_mem_cache_reader.get());
	}
	if (on_disk_cache_reader != nullptr) {
		cache_readers.emplace_back(on_disk_cache_reader.get());
	}
	return cache_readers;
}

void CacheReaderManager::ClearCache() {
	if (noop_cache_reader != nullptr) {
		noop_cache_reader->ClearCache();
	}
	if (in_mem_cache_reader != nullptr) {
		in_mem_cache_reader->ClearCache();
	}
	if (on_disk_cache_reader != nullptr) {
		on_disk_cache_reader->ClearCache();
	}
}

void CacheReaderManager::ClearCache(const string &fname) {
	if (noop_cache_reader != nullptr) {
		noop_cache_reader->ClearCache(fname);
	}
	if (in_mem_cache_reader != nullptr) {
		in_mem_cache_reader->ClearCache(fname);
	}
	if (on_disk_cache_reader != nullptr) {
		on_disk_cache_reader->ClearCache(fname);
	}
}

void CacheReaderManager::Reset() {
	noop_cache_reader.reset();
	in_mem_cache_reader.reset();
	on_disk_cache_reader.reset();
	internal_cache_reader = nullptr;
}

} // namespace duckdb

// ObjectCache-backed in-memory data block cache.

#pragma once

#include <cstdint>
#include <functional>
#include <utility>

#include "duckdb/common/map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "in_mem_cache_block.hpp"
#include "in_memory_data_cache_storage.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

namespace duckdb {

// Forward declarations.
class DatabaseInstance;
struct CacheHttpfsDataBlock;

// ObjectCache-backed in-memory data block cache.
//
// Blocks are page-aligned, allocated by the reader. Byte-cap LRU eviction is delegated to
// ObjectCache. A metadata-only map is kept for status queries.
class ObjectCacheStorage final : public InMemoryDataCacheStorage, public enable_shared_from_this<ObjectCacheStorage> {
public:
	// @param timeout_millisec: 0 disables lazy timeout-based eviction; ObjectCache LRU still applies.
	ObjectCacheStorage(DatabaseInstance &db_instance, uint64_t timeout_millisec);

	ObjectCacheStorage(const ObjectCacheStorage &) = delete;
	ObjectCacheStorage &operator=(const ObjectCacheStorage &) = delete;

	~ObjectCacheStorage() override;

	void Put(InMemCacheBlock key, PageAlignedDataChunk chunk, string version_tag) override;
	unique_ptr<PinnedBlock> Get(const InMemCacheBlock &key) override;
	bool Delete(const InMemCacheBlock &key) override;
	void Clear() override;
	void Clear(const InMemCacheBlock &start_key, std::function<bool(const InMemCacheBlock &)> filter) override;
	vector<InMemCacheBlock> Keys() const override;
	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> Take() override;

	void OnEntryDestroyed(const InMemCacheBlock &key, const CacheHttpfsDataBlock *block) noexcept;

private:
	struct EntryMeta {
		string oc_key;
		idx_t length = 0;
		string version_tag;
		uint64_t insertion_time_ms = 0;
		// Identity of the block currently installed under `oc_key`. The destructor compares against
		// this so a lingering ~CacheHttpfsDataBlock from a previously-displaced block cannot evict
		// the metadata of the block that replaced it.
		const CacheHttpfsDataBlock *block_ptr = nullptr;
	};

	DatabaseInstance &db_instance;
	const uint64_t timeout_millisec;

	mutable concurrency::mutex mu;
	map<InMemCacheBlock, EntryMeta, InMemCacheBlockLess> entries DUCKDB_GUARDED_BY(mu);
};

} // namespace duckdb

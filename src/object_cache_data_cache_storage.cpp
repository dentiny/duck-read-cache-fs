#include "object_cache_data_cache_storage.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "page_aligned_data_chunk.hpp"
#include "time_utils.hpp"
#include "url_utils.hpp"

#include <cstdint>
#include <utility>

namespace duckdb {

// Inner ObjectCache entry that owns the page-aligned chunk.
struct CacheHttpfsDataBlock : public ObjectCacheEntry {
	static constexpr const char *OBJECT_TYPE = "CacheHttpfsDataBlock";

	InMemCacheBlock key;
	weak_ptr<ObjectCacheStorage> storage;
	PageAlignedDataChunk data;
	uint64_t block_id;

	CacheHttpfsDataBlock(InMemCacheBlock key_p, weak_ptr<ObjectCacheStorage> storage_p, PageAlignedDataChunk data_p,
	                     uint64_t block_id_p)
	    : key(std::move(key_p)), storage(std::move(storage_p)), data(std::move(data_p)), block_id(block_id_p) {
	}

	~CacheHttpfsDataBlock() override {
		if (auto s = storage.lock()) {
			s->OnEntryDestroyed(key, block_id);
		}
	}

	string GetObjectType() override {
		return OBJECT_TYPE;
	}
	static string ObjectType() {
		return OBJECT_TYPE;
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(data.length);
	}
};

namespace {

string MakeObjCacheKey(const InMemCacheBlock &key) {
	const SanitizedCachePath sanitized {key.fname};
	return StringUtil::Format("cache_httpfs/data/%s:%llu:%llu", sanitized.Path(), key.start_off, key.blk_size);
}

} // namespace

ObjectCacheStorage::ObjectCacheStorage(DatabaseInstance &db_instance_p, uint64_t timeout_millisec_p)
    : db_instance(db_instance_p), timeout_millisec(timeout_millisec_p) {
}

// ObjectCacheStorage is owned  by ObjectCache, and the destructor is only called when object cache destructs.
// So we cannot (and there's no reason to)delete cache entries here, otherwise invalid memory access.
ObjectCacheStorage::~ObjectCacheStorage() noexcept = default;

void ObjectCacheStorage::Put(InMemCacheBlock key, PageAlignedDataChunk chunk, string version_tag) {
	ALWAYS_ASSERT(chunk.length > 0);

	string obj_cache_key = MakeObjCacheKey(key);
	const idx_t length = chunk.length;
	const uint64_t now = NumericCast<uint64_t>(GetSteadyNowMilliSecSinceEpoch());
	const uint64_t block_id = next_block_id.fetch_add(1);

	auto block = make_shared_ptr<CacheHttpfsDataBlock>(key, weak_ptr<ObjectCacheStorage>(shared_from_this()),
	                                                   std::move(chunk), block_id);

	EntryMeta meta;
	meta.obj_cache_key = obj_cache_key;
	meta.length = length;
	meta.version_tag = std::move(version_tag);
	meta.insertion_time_ms = now;
	meta.block_id = block_id;

	db_instance.GetObjectCache().Put(std::move(obj_cache_key), std::move(block));
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		entries[std::move(key)] = std::move(meta);
	}
}

optional<PinnedBlock> ObjectCacheStorage::Get(const InMemCacheBlock &key, const string &expected_version_tag) {
	string obj_cache_key;
	bool stale = false;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		auto it = entries.find(key);
		if (it == entries.end()) {
			return nullopt;
		}
		obj_cache_key = it->second.obj_cache_key;
		if (!PinnedBlock::ValidateVersionTag(it->second.version_tag, expected_version_tag)) {
			stale = true;
		} else if (timeout_millisec > 0) {
			const uint64_t now = static_cast<uint64_t>(GetSteadyNowMilliSecSinceEpoch());
			if (now - it->second.insertion_time_ms > timeout_millisec) {
				stale = true;
			}
		}
	}

	auto &obj_cache = db_instance.GetObjectCache();
	if (stale) {
		// `entries` is deleted by object cache entry deletion callback.
		obj_cache.Delete(obj_cache_key);
		return nullopt;
	}

	auto block = obj_cache.Get<CacheHttpfsDataBlock>(obj_cache_key);
	if (block == nullptr) {
		return nullopt;
	}

	const PageAlignedDataChunk *chunk_ptr = &block->data;
	shared_ptr<void> keep_alive = std::move(block);
	return PinnedBlock {std::move(keep_alive), chunk_ptr};
}

bool ObjectCacheStorage::Delete(const InMemCacheBlock &key) {
	string obj_cache_key;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		auto it = entries.find(key);
		if (it == entries.end()) {
			return false;
		}
		// Don't move the object cache key out, since we could have concurrent access.
		obj_cache_key = it->second.obj_cache_key;
	}
	// `entries` is deleted by object cache entry deletion callback.
	db_instance.GetObjectCache().Delete(obj_cache_key);
	return true;
}

void ObjectCacheStorage::Clear() {
	auto &obj_cache = db_instance.GetObjectCache();
	vector<string> obj_cache_keys;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		obj_cache_keys.reserve(entries.size());
		for (auto &kv : entries) {
			// Don't move the object cache key out, since we could have concurrent access.
			obj_cache_keys.emplace_back(kv.second.obj_cache_key);
		}
	}
	// `entries` is deleted by object cache entry deletion callback.
	for (const auto &obj_cache_key : obj_cache_keys) {
		obj_cache.Delete(obj_cache_key);
	}
}

void ObjectCacheStorage::Clear(const InMemCacheBlock &start_key, std::function<bool(const InMemCacheBlock &)> filter) {
	vector<string> obj_cache_keys;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		for (auto it = entries.lower_bound(start_key); it != entries.end(); ++it) {
			if (!filter(it->first)) {
				break;
			}
			// Don't move the object cache key out, since we could have concurrent access.
			obj_cache_keys.emplace_back(it->second.obj_cache_key);
		}
	}
	auto &obj_cache = db_instance.GetObjectCache();
	for (const auto &obj_cache_key : obj_cache_keys) {
		obj_cache.Delete(obj_cache_key);
	}
}

vector<InMemCacheBlock> ObjectCacheStorage::Keys() const {
	const concurrency::lock_guard<concurrency::mutex> lock(mu);
	vector<InMemCacheBlock> keys;
	keys.reserve(entries.size());
	for (const auto &kv : entries) {
		keys.emplace_back(kv.first);
	}
	return keys;
}

vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> ObjectCacheStorage::Take() {
	struct Snapshot {
		InMemCacheBlock key;
		string obj_cache_key;
		string version_tag;
	};
	vector<Snapshot> snapshot;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		snapshot.reserve(entries.size());
		for (const auto &kv : entries) {
			snapshot.emplace_back(Snapshot {kv.first, kv.second.obj_cache_key, kv.second.version_tag});
		}
	}

	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> result;
	result.reserve(snapshot.size());
	auto &obj_cache = db_instance.GetObjectCache();
	for (auto &snap : snapshot) {
		auto block = obj_cache.Get<CacheHttpfsDataBlock>(snap.obj_cache_key);
		if (block == nullptr) {
			continue;
		}
		auto entry = make_shared_ptr<InMemCacheDataEntry>();
		entry->data = std::move(block->data);
		entry->version_tag = std::move(snap.version_tag);
		result.emplace_back(std::move(snap.key), std::move(entry));
		obj_cache.Delete(snap.obj_cache_key);
	}
	return result;
}

void ObjectCacheStorage::OnEntryDestroyed(const InMemCacheBlock &key, uint64_t block_id) noexcept {
	const concurrency::lock_guard<concurrency::mutex> lock(mu);
	auto it = entries.find(key);
	if (it == entries.end()) {
		return;
	}
	if (it->second.block_id != block_id) {
		// A newer block has replaced this one; preserve the new metadata.
		return;
	}
	entries.erase(it);
}

} // namespace duckdb

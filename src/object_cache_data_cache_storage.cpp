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

namespace {

// Inner ObjectCache entry that owns the page-aligned chunk.
struct CacheHttpfsDataBlock : public ObjectCacheEntry {
	static constexpr const char *OBJECT_TYPE = "CacheHttpfsDataBlock";

	InMemCacheBlock key;
	weak_ptr<ObjectCacheStorage> storage;
	PageAlignedDataChunk data;

	CacheHttpfsDataBlock(InMemCacheBlock key_p, weak_ptr<ObjectCacheStorage> storage_p, PageAlignedDataChunk data_p)
	    : key(std::move(key_p)), storage(std::move(storage_p)), data(std::move(data_p)) {
	}

	~CacheHttpfsDataBlock() override {
		if (auto s = storage.lock()) {
			s->OnEntryDestroyed(key, this);
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

string MakeOcKey(const InMemCacheBlock &key) {
	const SanitizedCachePath sanitized {key.fname};
	return StringUtil::Format("cache_httpfs/data/%s:%llu:%llu", sanitized.Path(), key.start_off, key.blk_size);
}

} // namespace

ObjectCacheStorage::ObjectCacheStorage(DatabaseInstance &db_instance_p, uint64_t timeout_millisec_p)
    : db_instance(db_instance_p), timeout_millisec(timeout_millisec_p) {
}

ObjectCacheStorage::~ObjectCacheStorage() {
	auto &oc = db_instance.GetObjectCache();
	for (const auto &kv : entries) {
		oc.Delete(kv.second.oc_key);
	}
}

void ObjectCacheStorage::Put(InMemCacheBlock key, PageAlignedDataChunk chunk, string version_tag) {
	ALWAYS_ASSERT(chunk.length > 0);

	string oc_key = MakeOcKey(key);
	const idx_t length = chunk.length;
	const uint64_t now = static_cast<uint64_t>(GetSteadyNowMilliSecSinceEpoch());

	// Wrap the chunk before touching OC so `weak_from_this()` is well-defined here.
	auto block =
	    make_shared_ptr<CacheHttpfsDataBlock>(key, weak_ptr<ObjectCacheStorage>(shared_from_this()), std::move(chunk));
	const CacheHttpfsDataBlock *block_ptr = block.get();

	// Install into OC without holding `mu`. If this displaces an existing entry under `oc_key`,
	// the displaced block's destructor fires on this thread (no other refs => refcount goes to 0
	// inside the LRU's replace path) and `OnEntryDestroyed` erases its map entry before we return.
	db_instance.GetObjectCache().Put(oc_key, std::move(block));

	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		EntryMeta meta;
		meta.oc_key = std::move(oc_key);
		meta.length = length;
		meta.version_tag = std::move(version_tag);
		meta.insertion_time_ms = now;
		meta.block_ptr = block_ptr;
		entries[std::move(key)] = std::move(meta);
	}
}

optional<PinnedBlock> ObjectCacheStorage::Get(const InMemCacheBlock &key) {
	string oc_key;
	string version_tag;
	bool stale = false;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		auto it = entries.find(key);
		if (it == entries.end()) {
			return nullopt;
		}
		oc_key = it->second.oc_key;
		version_tag = it->second.version_tag;
		if (timeout_millisec > 0) {
			const uint64_t now = static_cast<uint64_t>(GetSteadyNowMilliSecSinceEpoch());
			if (now - it->second.insertion_time_ms > timeout_millisec) {
				stale = true;
			}
		}
	}

	auto &oc = db_instance.GetObjectCache();
	if (stale) {
		// Outside the lock: destructor finalises the map removal.
		oc.Delete(oc_key);
		return nullopt;
	}

	auto block = oc.Get<CacheHttpfsDataBlock>(oc_key);
	if (block == nullptr) {
		// OC LRU just evicted; destructor either fired already (map cleaned) or will fire when
		// the lingering shared_ref drops. Treat as a miss.
		return nullopt;
	}

	const PageAlignedDataChunk *chunk_ptr = &block->data;
	shared_ptr<void> keep_alive = std::move(block);
	return PinnedBlock {std::move(keep_alive), chunk_ptr, std::move(version_tag)};
}

bool ObjectCacheStorage::Delete(const InMemCacheBlock &key) {
	string oc_key;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		auto it = entries.find(key);
		if (it == entries.end()) {
			return false;
		}
		// The map entry is about to be erased by `OnEntryDestroyed`; safe to steal the string.
		// A racing `Get` on the same key would see an empty `oc_key` and just miss, which is fine
		// since the block is being deleted anyway.
		oc_key = std::move(it->second.oc_key);
	}
	db_instance.GetObjectCache().Delete(oc_key);
	return true;
}

void ObjectCacheStorage::Clear() {
	// Snapshot oc_keys under the lock, then call OC outside it (destructors re-enter `mu`).
	// We move the strings out of the map: callbacks erase the entries shortly after anyway, so
	// the moved-from values are inert; a racing `Get` would observe an empty `oc_key` => miss,
	// which is the desired behaviour during Clear.
	vector<string> oc_keys;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		oc_keys.reserve(entries.size());
		for (auto &kv : entries) {
			oc_keys.push_back(std::move(kv.second.oc_key));
		}
	}
	auto &oc = db_instance.GetObjectCache();
	for (const auto &oc_key : oc_keys) {
		oc.Delete(oc_key);
	}
	// Map cleanup happens via destructor callbacks. Entries pinned by in-flight `Get`s linger
	// until those readers release; the map drains as those refs drop.
}

void ObjectCacheStorage::Clear(const InMemCacheBlock &start_key, std::function<bool(const InMemCacheBlock &)> filter) {
	vector<string> oc_keys;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		for (auto it = entries.lower_bound(start_key); it != entries.end(); ++it) {
			if (!filter(it->first)) {
				break;
			}
			oc_keys.push_back(std::move(it->second.oc_key));
		}
	}
	auto &oc = db_instance.GetObjectCache();
	for (const auto &oc_key : oc_keys) {
		oc.Delete(oc_key);
	}
}

vector<InMemCacheBlock> ObjectCacheStorage::Keys() const {
	const concurrency::lock_guard<concurrency::mutex> lock(mu);
	vector<InMemCacheBlock> keys;
	keys.reserve(entries.size());
	for (const auto &kv : entries) {
		keys.push_back(kv.first);
	}
	return keys;
}

vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> ObjectCacheStorage::Take() {
	struct Snapshot {
		InMemCacheBlock key;
		string oc_key;
		string version_tag;
	};
	vector<Snapshot> snapshot;
	{
		const concurrency::lock_guard<concurrency::mutex> lock(mu);
		snapshot.reserve(entries.size());
		for (auto &kv : entries) {
			// The map entries will be erased by `OnEntryDestroyed` after we `oc.Delete`; safe to
			// move oc_key / version_tag out now.
			snapshot.push_back(Snapshot {kv.first, std::move(kv.second.oc_key), std::move(kv.second.version_tag)});
		}
	}

	vector<std::pair<InMemCacheBlock, shared_ptr<InMemCacheDataEntry>>> result;
	result.reserve(snapshot.size());
	auto &oc = db_instance.GetObjectCache();
	for (auto &snap : snapshot) {
		auto block = oc.Get<CacheHttpfsDataBlock>(snap.oc_key);
		if (block == nullptr) {
			continue;
		}
		auto entry = make_shared_ptr<InMemCacheDataEntry>();
		// Move the chunk out before OC drops its ref; OC's accounting will still see the original
		// reserved bytes when the buffer pool reservation is released by `~BufferPoolPayload`.
		entry->data = std::move(block->data);
		entry->version_tag = std::move(snap.version_tag);
		result.emplace_back(std::move(snap.key), std::move(entry));
		oc.Delete(snap.oc_key);
		// Drop our local ref; ~CacheHttpfsDataBlock fires and `OnEntryDestroyed` erases the map entry.
		block.reset();
	}
	return result;
}

void ObjectCacheStorage::OnEntryDestroyed(const InMemCacheBlock &key, const CacheHttpfsDataBlock *block) noexcept {
	const concurrency::lock_guard<concurrency::mutex> lock(mu);
	auto it = entries.find(key);
	if (it == entries.end()) {
		return;
	}
	if (it->second.block_ptr != block) {
		// A newer block has replaced this one; preserve the new metadata.
		return;
	}
	entries.erase(it);
}

} // namespace duckdb

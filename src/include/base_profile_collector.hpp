#pragma once

#include <string>
#include <utility>

#include "cache_entry_info.hpp"
#include "cache_filesystem_config.hpp"

namespace duckdb {

// A commonly seen way to lay filesystem features is decorator pattern, with each feature as a new class and layer.
// In the ideal world, profiler should be achieved as another layer, just like how we implement cache filesystem; but
// that requires us to implant more config settings and global variables. For simplicity (since we only target cache
// filesystem in the extension), profiler collector is used as a data member for cache filesystem.
class BaseProfileCollector {
public:
	enum class CacheEntity {
		kMetadata,   // File metadata.
		kData,       // File data block.
		kFileHandle, // File handle.
		kGlob,       // Glob.
		kUnknown,
	};
	enum class CacheAccess {
		kCacheHit,
		kCacheMiss,
	};
	enum class IoOperation {
		kOpen,
		kRead,
		kGlob,
		kUnknown,
	};
	static constexpr auto kCacheEntityCount = static_cast<size_t>(CacheEntity::kUnknown);
	static constexpr auto kIoOperationCount = static_cast<size_t>(IoOperation::kUnknown);

	// Operation names, indexed by operation enums.
	static const std::array<const char *, kIoOperationCount> OPER_NAMES;
	// Cache entity name, indexed by cache entity enum.
	static const std::array<const char *, kCacheEntityCount> CACHE_ENTITY_NAMES;

	BaseProfileCollector() = default;
	virtual ~BaseProfileCollector() = default;
	BaseProfileCollector(const BaseProfileCollector &) = delete;
	BaseProfileCollector &operator=(const BaseProfileCollector &) = delete;

	// Get an ID which uniquely identifies current operation.
	virtual std::string GenerateOperId() const = 0;
	// Record the start of operation [io_oper] with operation identifier [oper_id].
	virtual void RecordOperationStart(IoOperation io_oper, const std::string &oper_id) = 0;
	// Record the finish of operation [io_oper] with operation identifier [oper_id].
	virtual void RecordOperationEnd(IoOperation io_oper, const std::string &oper_id) = 0;
	// Record cache access condition.
	virtual void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) = 0;
	// Get profiler type.
	virtual std::string GetProfilerType() = 0;
	// Get cache access information.
	// It's guaranteed that access info are returned in the order of and are size of [CacheEntity].
	virtual vector<CacheAccessInfo> GetCacheAccessInfo() const = 0;
	// Set cache reader type.
	void SetCacheReaderType(std::string cache_reader_type_p) {
		cache_reader_type = std::move(cache_reader_type_p);
	}
	// Reset profile stats.
	virtual void Reset() = 0;
	// Get human-readable aggregated profile collection, and its latest completed IO operation timestamp.
	virtual std::pair<std::string /*stats*/, uint64_t /*timestamp*/> GetHumanReadableStats() = 0;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	std::string cache_reader_type = "";
};

class NoopProfileCollector final : public BaseProfileCollector {
public:
	NoopProfileCollector() = default;
	~NoopProfileCollector() override = default;

	std::string GenerateOperId() const override {
		return "";
	}
	void RecordOperationStart(IoOperation io_oper, const std::string &oper_id) override {
	}
	void RecordOperationEnd(IoOperation io_oper, const std::string &oper_id) override {
	}
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) override {
	}
	std::string GetProfilerType() override {
		return *NOOP_PROFILE_TYPE;
	}
	vector<CacheAccessInfo> GetCacheAccessInfo() const override {
		vector<CacheAccessInfo> cache_access_info;
		cache_access_info.resize(kCacheEntityCount);
		for (size_t idx = 0; idx < kCacheEntityCount; ++idx) {
			cache_access_info[idx].cache_type = CACHE_ENTITY_NAMES[idx];
		}
		return cache_access_info;
	}
	void Reset() override {};
	std::pair<std::string, uint64_t> GetHumanReadableStats() override {
		return std::make_pair("(noop profile collector)", /*timestamp=*/0);
	}
};

} // namespace duckdb

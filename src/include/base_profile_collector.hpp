#pragma once

#include <string>
#include <utility>

#include "cache_filesystem_config.hpp"

namespace duckdb {

// A commonly seen way to lay filesystem features is decorator pattern, with each feature as a new class and layer.
// In the ideal world, profiler should be achieved as another layer, just like how we implement cache filesystem; but
// that requires us to implant more config settings and global variables. For simplicity (since we only target cache
// filesystem in the extension), profiler collector is used as a data member for cache filesystem.
class BaseProfileCollector {
public:
	enum class CacheEntity {
		kMetadata, // File metadata.
		kData,     // File data block.
	};
	enum class CacheAccess {
		kCacheHit,
		kCacheMiss,
	};

	BaseProfileCollector() = default;
	virtual ~BaseProfileCollector() = default;

	// Get an ID which uniquely identifies current operation.
	virtual std::string GetOperId() const = 0;
	// Record the start of operation [oper].
	virtual void RecordOperationStart(const std::string &oper) = 0;
	// Record the finish of operation [oper].
	virtual void RecordOperationEnd(const std::string &oper) = 0;
	// Record cache access condition.
	virtual void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) = 0;
	// Get profiler type.
	virtual std::string GetProfilerType() = 0;
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

	std::string GetOperId() const override {
		return "";
	}
	void RecordOperationStart(const std::string &oper) override {
	}
	void RecordOperationEnd(const std::string &oper) override {
	}
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) override {
	}
	std::string GetProfilerType() override {
		return NOOP_PROFILE_TYPE;
	}
	void Reset() override {};
	std::pair<std::string, uint64_t> GetHumanReadableStats() override {
		return std::make_pair("(noop profile collector)", /*timestamp=*/0);
	}
};

} // namespace duckdb

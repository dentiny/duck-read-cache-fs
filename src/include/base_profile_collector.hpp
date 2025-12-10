#pragma once

#include <utility>

#include "cache_entry_info.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "io_operations.hpp"

namespace duckdb {

// Forward declaration.
class BaseProfileCollector;

// A RAII guard to measure latency for IO operations.
class LatencyGuard {
public:
	// Constructor which doesn't report latency.
	LatencyGuard(BaseProfileCollector &profile_collector_p);
	// Constructor which auto reports latency metrics.
	LatencyGuard(BaseProfileCollector &profile_collector_p, IoOperation io_operation_p);
	~LatencyGuard();

	LatencyGuard(const LatencyGuard &) = delete;
	LatencyGuard &operator=(const LatencyGuard &) = delete;
	LatencyGuard(LatencyGuard &&) = default;
	LatencyGuard &operator=(LatencyGuard &&) = delete;

private:
	BaseProfileCollector &profile_collector;
	IoOperation io_operation = IoOperation::kUnknown;
	int64_t start_timestamp = 0;
};

// A commonly seen way to lay filesystem features is decorator pattern, with each feature as a new class and layer.
// In the ideal world, profiler should be achieved as another layer, just like how we implement cache filesystem; but
// that requires us to implant more config settings and global variables. For simplicity (since we only target cache
// filesystem in the extension), profiler collector is used as a data member for cache filesystem.
class BaseProfileCollector {
public:
	BaseProfileCollector() = default;
	virtual ~BaseProfileCollector() = default;
	BaseProfileCollector(const BaseProfileCollector &) = delete;
	BaseProfileCollector &operator=(const BaseProfileCollector &) = delete;

	// Record the start of operation [io_oper] and return [`LatencyGuard`] for auto latency record.
	virtual LatencyGuard RecordOperationStart(IoOperation io_oper) = 0;
	// Record the finish of operation [io_oper] with the elapse latency.
	virtual void RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec) = 0;
	// Record cache access condition with byte count.
	virtual void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access, idx_t byte_count) = 0;
	// Record cache size and actual bytes access.
	virtual void RecordActualCacheRead(idx_t cache_size, idx_t actual_bytes) = 0;
	// Get profiler type.
	virtual string GetProfilerType() = 0;
	// Get cache access information.
	// It's guaranteed that access info are returned in the order of and are size of [CacheEntity].
	virtual vector<CacheAccessInfo> GetCacheAccessInfo() const = 0;
	// Set cache reader type.
	void SetCacheReaderType(string cache_reader_type_p) {
		cache_reader_type = std::move(cache_reader_type_p);
	}
	// Reset profile stats.
	virtual void Reset() = 0;
	// Get human-readable aggregated profile collection, and its latest completed IO operation timestamp.
	virtual std::pair<string /*stats*/, uint64_t /*timestamp*/> GetHumanReadableStats() = 0;

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
	string cache_reader_type = "";
};

class NoopProfileCollector final : public BaseProfileCollector {
public:
	NoopProfileCollector() = default;
	~NoopProfileCollector() override = default;

	LatencyGuard RecordOperationStart(IoOperation io_oper) override {
		return LatencyGuard {*this, std::move(io_oper)};
	}
	void RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec) override {
	}
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access, idx_t byte_count) override {
	}
	void RecordActualCacheRead(idx_t cache_size, idx_t actual_bytes) override {
	}
	string GetProfilerType() override {
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
	std::pair<string, uint64_t> GetHumanReadableStats() override {
		return std::make_pair("(noop profile collector)", /*timestamp=*/0);
	}
};

} // namespace duckdb

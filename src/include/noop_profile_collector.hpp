#pragma once

#include <atomic>

#include "base_profile_collector.hpp"

namespace duckdb {

class NoopProfileCollector final : public BaseProfileCollector {
public:
	NoopProfileCollector() = default;
	~NoopProfileCollector() override = default;

	LatencyGuard RecordOperationStart(IoOperation io_oper) override;
	void RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec) override {
	}
	void RecordCacheAccess(CacheEntity cache_entity, CacheAccess cache_access) override {
	}
	void RecordActualCacheRead(idx_t cache_size, idx_t actual_bytes) override {
	}
	string GetProfilerType() override {
		return *NOOP_PROFILE_TYPE;
	}
	vector<CacheAccessInfo> GetCacheAccessInfo() const override;
	void Reset() override {
		latest_timestamp = 0;
	};
	std::pair<string, uint64_t> GetHumanReadableStats() override {
		return std::make_pair("(noop profile collector)", static_cast<uint64_t>(latest_timestamp.load()));
	}

private:
	std::atomic<uint64_t> latest_timestamp {0};
};

} // namespace duckdb

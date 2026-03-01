#include "catch/catch.hpp"

#include "base_profile_collector.hpp"
#include "cache_entry_info.hpp"
#include "io_operations.hpp"

using namespace duckdb; // NOLINT

namespace {

// Profile collector that counts RecordOperationEnd calls. Used to verify guard semantics.
class CounterProfileCollector final : public BaseProfileCollector {
public:
	LatencyGuard RecordOperationStart(IoOperation io_oper) override {
		return LatencyGuard {*this, io_oper};
	}
	void RecordOperationEnd(IoOperation io_oper, int64_t latency_millisec) override {
		(void)io_oper;          // Suppress unused parameter warning.
		(void)latency_millisec; // Suppress unused parameter warning.
		++record_end_count;
	}
	void RecordCacheAccess(CacheEntity, CacheAccess) override {
	}
	void RecordActualCacheRead(idx_t, idx_t) override {
	}
	void RecordBytesWritten(idx_t) override {
	}
	string GetProfilerType() override {
		return "counter";
	}
	vector<CacheAccessInfo> GetCacheAccessInfo() const override {
		return {};
	}
	void Reset() override {
		record_end_count = 0;
	}
	std::pair<string, uint64_t> GetHumanReadableStats() override {
		return {"", 0};
	}
	size_t GetRecordOperationEndCount() const {
		return record_end_count;
	}

private:
	size_t record_end_count = 0;
};

// Create a latency guard.
LatencyGuard RecordOperationStart(BaseProfileCollector &collector) {
	return collector.RecordOperationStart(IoOperation::kRead);
}
} // namespace

TEST_CASE("LatencyGuard single guard calls RecordOperationEnd once", "[latency guard]") {
	CounterProfileCollector collector;
	{
		auto guard = RecordOperationStart(collector);
	}
	REQUIRE(collector.GetRecordOperationEndCount() == 1);
}

TEST_CASE("LatencyGuard move constructor - RecordOperationEnd only called once", "[latency guard]") {
	CounterProfileCollector collector;
	{
		auto guard_a = RecordOperationStart(collector);
		auto guard_b = std::move(guard_a);
	}
	REQUIRE(collector.GetRecordOperationEndCount() == 1);
}

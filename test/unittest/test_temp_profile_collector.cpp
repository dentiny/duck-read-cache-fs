// Unit test for TempProfileCollector: record latency via LatencyGuard and assert exactly one data point.

#include "catch/catch.hpp"

#include "duckdb/common/string_util.hpp"
#include "io_operations.hpp"
#include "temp_profile_collector.hpp"

using namespace duckdb; // NOLINT

namespace {
// Create a latency guard.
LatencyGuard RecordOperationStart(TempProfileCollector &collector) {
	return collector.RecordOperationStart(IoOperation::kRead);
}
} // namespace

TEST_CASE("TempProfileCollector records one latency data point via LatencyGuard", "[temp profile collector]") {
	TempProfileCollector collector;
	{
		auto latency_guard = RecordOperationStart(collector);
	}
	const auto stats_pair = collector.GetHumanReadableStats();
	std::cerr << stats_pair.first << std::endl;
	std::cerr << stats_pair.second << std::endl;
}

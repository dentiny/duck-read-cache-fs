#include "base_profile_collector.hpp"

#include "time_utils.hpp"

namespace duckdb {

LatencyGuard::LatencyGuard(BaseProfileCollector &profile_collector_p) : profile_collector(profile_collector_p) {
}

LatencyGuard::LatencyGuard(BaseProfileCollector &profile_collector_p, IoOperation io_operation_p)
    : profile_collector(profile_collector_p), io_operation(io_operation_p),
      start_timestamp(GetSteadyNowMilliSecSinceEpoch()) {
}

LatencyGuard::~LatencyGuard() {
	if (io_operation == IoOperation::kUnknown) {
		return;
	}
	const auto now = GetSteadyNowMilliSecSinceEpoch();
	const auto latency_millisec = now - start_timestamp;
	profile_collector.RecordOperationEnd(io_operation, latency_millisec);
}

} // namespace duckdb

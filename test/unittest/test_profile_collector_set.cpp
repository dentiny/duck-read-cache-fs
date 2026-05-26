#include "catch/catch.hpp"

#include "base_profile_collector.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/exception.hpp"
#include "noop_profile_collector.hpp"
#include "temp_profile_collector.hpp"

using namespace duckdb; // NOLINT

namespace {
constexpr connection_t kConnId = 42;
} // namespace

TEST_CASE("SetProfileCollector with same type does not replace stored collector", "[profile collector]") {
	InstanceProfileCollectorManager manager;

	manager.SetProfileCollector(kConnId, *NOOP_PROFILE_TYPE);
	auto &first = manager.GetProfileCollectorOrThrow(kConnId);
	auto *first_addr = &first;

	manager.SetProfileCollector(kConnId, *NOOP_PROFILE_TYPE);
	auto &second = manager.GetProfileCollectorOrThrow(kConnId);
	REQUIRE(&second == first_addr);
}

TEST_CASE("SetProfileCollector switching between distinct profile types replaces the stored collector",
          "[profile collector]") {
	InstanceProfileCollectorManager manager;

	manager.SetProfileCollector(kConnId, *NOOP_PROFILE_TYPE);
	auto &noop_collector = manager.GetProfileCollectorOrThrow(kConnId);
	REQUIRE(noop_collector.GetProfilerType() == *NOOP_PROFILE_TYPE);

	manager.SetProfileCollector(kConnId, *TEMP_PROFILE_TYPE);
	auto &temp_collector = manager.GetProfileCollectorOrThrow(kConnId);
	REQUIRE(temp_collector.GetProfilerType() == *TEMP_PROFILE_TYPE);

	manager.SetProfileCollector(kConnId, *NOOP_PROFILE_TYPE);
	auto &back_to_noop = manager.GetProfileCollectorOrThrow(kConnId);
	REQUIRE(back_to_noop.GetProfilerType() == *NOOP_PROFILE_TYPE);
}

TEST_CASE("SetProfileCollector rejects 'duckdb' as an invalid profile type", "[profile collector]") {
	InstanceProfileCollectorManager manager;
	REQUIRE_THROWS_AS(manager.SetProfileCollector(kConnId, "duckdb"), InvalidInputException);
}

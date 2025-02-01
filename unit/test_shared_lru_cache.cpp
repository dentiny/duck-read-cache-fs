#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <string>

#include "lru_cache.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("PutAndGetSameKey", "[shared lru test]") {
  ThreadSafeSharedLruCache<std::string, std::string> cache{1};

  // No value initially.
  auto val = cache.Get("1");
  REQUIRE(val == nullptr);

  // Check put and get.
  cache.Put("1", std::make_shared<std::string>("1"));
  val = cache.Get("1");
  REQUIRE(val != nullptr);
  REQUIRE(*val == "1");

  // Check key eviction.
  cache.Put("2", std::make_shared<std::string>("2"));
  val = cache.Get("1");
  REQUIRE(val == nullptr);
  val = cache.Get("2");
  REQUIRE(val != nullptr);
  REQUIRE(*val == "2");

  // Check deletion.
  REQUIRE(!cache.Delete("1"));
  REQUIRE(cache.Delete("2"));
  val = cache.Get("2");
  REQUIRE(val == nullptr);
}

int main(int argc, char **argv) {
  int result = Catch::Session().run(argc, argv);
  return result;
}

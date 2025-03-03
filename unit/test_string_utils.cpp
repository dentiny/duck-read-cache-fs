#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "string_utils.hpp"

using namespace duckdb;  // NOLINT

namespace {
constexpr std::size_t kBufSize = 10;
}  // namespace

TEST_CASE("StringUtilsTest", "ImmutableBuffer") {
    ImmutableBuffer buffer(kBufSize);
    std::memmove(/*dst=*/const_cast<char*>(buffer.data()), /*src=*/"helloworld", kBufSize);

    // Inequal case.
    const std::string content1 = "hello world";
    REQUIRE(buffer != content1);
    REQUIRE(content1 != buffer);

    // Equal case.
    const std::string content2 = "helloworld";
    REQUIRE(buffer == content2);
    REQUIRE(content2 == buffer);
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

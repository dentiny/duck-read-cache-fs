#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "no_destructor.hpp"

#include <string>

using namespace duckdb; // NOLINT

TEST_CASE("NoDestructor test", "[no destructor test]") {
	const std::string s = "helloworld";

	// Default constructor.
	{
		NoDestructor<std::string> content {};
		REQUIRE(*content == "");
	}

	// Construct by const reference.
	{
		NoDestructor<std::string> content {s};
		REQUIRE(*content == s);
	}

	// Construct by rvalue reference.
	{
		std::string another_str = "helloworld";
		NoDestructor<std::string> content {std::move(another_str)};
		REQUIRE(*content == s);
	}

	// Construct by ctor with multiple arguments.
	{
		NoDestructor<std::string> content {s.begin(), s.end()};
		REQUIRE(*content == "helloworld");
	}

	// Access internal object.
	{
		NoDestructor<std::string> content {s.begin(), s.end()};
		(*content)[0] = 'b';
		(*content)[1] = 'c';
		REQUIRE(*content == "bclloworld");
	}

	// Reassign.
	{
		NoDestructor<std::string> content {s.begin(), s.end()};
		*content = "worldhello";
		REQUIRE(*content == "worldhello");
	}
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/string.hpp"
#include "no_destructor.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("NoDestructor test", "[no destructor test]") {
	const string s = "helloworld";

	// Default constructor.
	{
		NoDestructor<string> content {};
		REQUIRE(*content == "");
	}

	// Construct by const reference.
	{
		NoDestructor<string> content {s};
		REQUIRE(*content == s);
	}

	// Construct by rvalue reference.
	{
		string another_str = "helloworld";
		NoDestructor<string> content {std::move(another_str)};
		REQUIRE(*content == s);
	}

	// Construct by ctor with multiple arguments.
	{
		NoDestructor<string> content {s.begin(), s.end()};
		REQUIRE(*content == "helloworld");
	}

	// Access internal object.
	{
		NoDestructor<string> content {s.begin(), s.end()};
		(*content)[0] = 'b';
		(*content)[1] = 'c';
		REQUIRE(*content == "bclloworld");
	}

	// Reassign.
	{
		NoDestructor<string> content {s.begin(), s.end()};
		*content = "worldhello";
		REQUIRE(*content == "worldhello");
	}
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}

// Unit test for ScopedDirectory.

#include "catch/catch.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/string_util.hpp"
#include "scoped_directory.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Test ScopedDirectory creates and removes directory", "[scoped directory test]") {
	const string test_dir = StringUtil::Format("/tmp/test_scoped_dir_%s", UUID::ToString(UUID::GenerateRandomUUID()));
	auto local_filesystem = LocalFileSystem::CreateLocal();

	// Directory doesn't exist before scoped directory.
	if (local_filesystem->DirectoryExists(test_dir)) {
		local_filesystem->RemoveDirectory(test_dir);
	}

	{
		ScopedDirectory dir(test_dir);
		REQUIRE(local_filesystem->DirectoryExists(test_dir));
		REQUIRE(dir.GetPath() == test_dir);
	}

	REQUIRE(!local_filesystem->DirectoryExists(test_dir));
}

TEST_CASE("Test ScopedDirectory creation with existing directory", "[scoped directory test]") {
	const string test_dir = StringUtil::Format("/tmp/test_scoped_dir_%s", UUID::ToString(UUID::GenerateRandomUUID()));
	auto local_filesystem = LocalFileSystem::CreateLocal();

	// Directory does exist before scoped directory.
	local_filesystem->CreateDirectory(test_dir);
	REQUIRE(local_filesystem->DirectoryExists(test_dir));

	{
		ScopedDirectory dir(test_dir);
		REQUIRE(local_filesystem->DirectoryExists(test_dir));
	}

	REQUIRE(!local_filesystem->DirectoryExists(test_dir));
}

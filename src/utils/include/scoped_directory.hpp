// ScopedDirectory is a RAII wrapper for directory management.
// It ensures a directory exists on construction and removes it on destruction.
//
// Example:
//   {
//     ScopedDirectory dir("/tmp/my_test_dir");
//     // Directory exists and will be removed when dir goes out of scope
//   }  // Directory is automatically removed

#pragma once

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class ScopedDirectory {
public:
	explicit ScopedDirectory(string directory_path_p);
	~ScopedDirectory();

	// Disable copy and move constructor and assignment.
	ScopedDirectory(const ScopedDirectory &) = delete;
	ScopedDirectory &operator=(const ScopedDirectory &) = delete;

	const string &GetPath() const {
		return directory_path;
	}

private:
	string directory_path;
};

} // namespace duckdb

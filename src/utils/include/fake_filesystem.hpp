// A fake filesystem for cache httpfs extension testing purpose.

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

// WARNING: fake filesystem is used for testing purpose and shouldn't be used in production.
class CacheHttpfsFakeFileSystem : public LocalFileSystem {
public:
	std::string GetName() const override {
		return "cache_httpfs_fake_filesystem";
	}
};

} // namespace duckdb

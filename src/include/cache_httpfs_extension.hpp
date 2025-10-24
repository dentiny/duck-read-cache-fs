#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class CacheHttpfsExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	string Name() override;
	string Version() const override;

private:
	// Cache httpfs automatically loads httpfs.
	unique_ptr<Extension> httpfs_extension;
};

} // namespace duckdb

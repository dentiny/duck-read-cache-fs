#pragma once

#include "duckdb.hpp"

#include <string>

namespace duckdb {

class CacheHttpfsExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;

private:
	unique_ptr<Extension> httpfs_extension;
};

} // namespace duckdb

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/main/extension.hpp"

namespace duckdb {

class CacheHttpfsExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	string Name() override;
	string Version() const override;
};

} // namespace duckdb

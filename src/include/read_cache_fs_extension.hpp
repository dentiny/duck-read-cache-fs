#pragma once

#include "duckdb.hpp"

#include <string>

namespace duckdb {

class ReadCacheFsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb

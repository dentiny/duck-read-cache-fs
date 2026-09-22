#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterCacheHttpfsFunctions(ExtensionLoader &loader);

} // namespace duckdb

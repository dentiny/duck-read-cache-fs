#include "cache_httpfs_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/setting_info.hpp"

namespace duckdb {

void SetCacheHttpfsExtensionOption(DatabaseInstance &instance, const string &name, Value value) {
	if (!StringUtil::StartsWith(name, "cache_httpfs_")) {
		throw InvalidInputException("Invalid cache_httpfs option name '%s'", name);
	}

	auto &config = DBConfig::GetConfig(instance);
	ExtensionOption option;
	if (!config.TryGetExtensionOption(name, option)) {
		throw InvalidInputException("cache_httpfs option '%s' is unavailable", name);
	}
	if (!option.setting_index.IsValid()) {
		throw InternalException("cache_httpfs option '%s' is not registered", name);
	}

	Connection connection(instance);
	auto target_value = value.CastAs(*connection.context, option.type);
	if (option.set_function) {
		option.set_function(*connection.context, SetScope::GLOBAL, target_value);
	}
	config.SetOption(option.setting_index.GetIndex(), std::move(target_value));
}

} // namespace duckdb

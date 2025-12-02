#include "cache_exclusion_utils.hpp"

#include <algorithm>

#include "cache_exclusion_manager.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace {
constexpr bool SUCCESS = true;

// Get database instance from expression state.
CacheExclusionManager *GetExclusionManager(ExpressionState &state) {
	auto *executor = state.root.executor;
	auto &client_context = executor->GetContext();
	auto &instance = *client_context.db.get();

	auto *inst_state = GetInstanceState(instance);
	if (inst_state == nullptr) {
		return nullptr;
	}

	return &inst_state->exclusion_manager;
}

//===--------------------------------------------------------------------===//
// List cache exclusion regex query function
//===--------------------------------------------------------------------===//

struct ListCacheExclusionRegexData : public GlobalTableFunctionState {
	vector<string> exclusion_regex_string;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

unique_ptr<FunctionData> ListCacheExclusionRegexQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(1);
	names.reserve(1);

	// Excluded cache regex.
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("cache_exclusion_regex");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> ListCacheExclusionRegexQueryFuncInit(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto result = make_uniq<ListCacheExclusionRegexData>();
	auto *state = GetInstanceState(*context.db);
	if (state) {
		result->exclusion_regex_string = state->exclusion_manager.GetExclusionRegex();
	}

	// Sort the results to ensure determinististism and testibility.
	std::sort(result->exclusion_regex_string.begin(), result->exclusion_regex_string.end());

	return std::move(result);
}

void ListCacheExclusionRegexQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<ListCacheExclusionRegexData>();

	// All entries have been emitted.
	if (data.offset >= data.exclusion_regex_string.size()) {
		return;
	}

	// Start filling in the result buffer.
	idx_t count = 0;
	while (data.offset < data.exclusion_regex_string.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.exclusion_regex_string[data.offset++];
		idx_t col = 0;

		// Registerd filesystem.
		output.SetValue(col++, count, entry);

		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void AddCacheExclusionRegex(const DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	string regex = args.GetValue(/*col_idx=*/0, /*index=*/0).ToString();

	auto *exclusion_manager = GetExclusionManager(state);
	if (exclusion_manager) {
		exclusion_manager->AddExclusionRegex(regex);
	}
	result.Reference(Value(SUCCESS));
}

void ResetCacheExclusionRegex(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto *exclusion_manager = GetExclusionManager(state);
	if (exclusion_manager) {
		exclusion_manager->ResetExclusionRegex();
	}
	result.Reference(Value(SUCCESS));
}

TableFunction ListCacheExclusionRegex() {
	TableFunction list_cache_exclusion_query_func {/*name=*/"cache_httpfs_list_exclusion_regex",
	                                               /*arguments=*/ {},
	                                               /*function=*/ListCacheExclusionRegexQueryTableFunc,
	                                               /*bind=*/ListCacheExclusionRegexQueryFuncBind,
	                                               /*init_global=*/ListCacheExclusionRegexQueryFuncInit};
	return list_cache_exclusion_query_func;
}

} // namespace duckdb

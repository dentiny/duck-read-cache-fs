#include "filesystem_status_query_function.hpp"

#include <algorithm>

#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// List registered filesystems query function
//===--------------------------------------------------------------------===//

struct ListFileSystemData : public GlobalTableFunctionState {
	vector<string> registered_filesystems;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

unique_ptr<FunctionData> ListFileSystemQueryFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(1);
	names.reserve(1);

	// Registered filesystems.
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("registered_filesystems");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> ListFileSystemQueryFuncInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<ListFileSystemData>();
	auto &entries_info = result->registered_filesystems;

	auto &duckdb_instance = context.db;
	auto &opener_filesystem = duckdb_instance->GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	auto filesystems = vfs.ListSubSystems();

	// Sort the results to ensure determinististism and testibility.
	std::sort(filesystems.begin(), filesystems.end());
	entries_info = std::move(filesystems);

	return std::move(result);
}

void ListFileSystemQueryTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<ListFileSystemData>();

	// All entries have been emitted.
	if (data.offset >= data.registered_filesystems.size()) {
		return;
	}

	// Start filling in the result buffer.
	idx_t count = 0;
	while (data.offset < data.registered_filesystems.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.registered_filesystems[data.offset++];
		idx_t col = 0;

		// Registerd filesystem.
		output.SetValue(col++, count, entry);

		count++;
	}
	output.SetCardinality(count);
}

} // namespace

TableFunction ListRegisteredFileSystemsQueryFunc() {
	TableFunction list_filesystems_query_func {/*name=*/"cache_httpfs_list_registered_filesystems",
	                                           /*arguments=*/ {},
	                                           /*function=*/ListFileSystemQueryTableFunc,
	                                           /*bind=*/ListFileSystemQueryFuncBind,
	                                           /*init_global=*/ListFileSystemQueryFuncInit};
	return list_filesystems_query_func;
}

} // namespace duckdb

#pragma once

#include "parallel_executor.hpp"

#include "duckdb/parallel/task_executor.hpp"

#include <functional>

namespace duckdb {

class DatabaseInstance;

class DuckDBTaskParallelExecutor final : public BaseParallelExecutor {
public:
	explicit DuckDBTaskParallelExecutor(DatabaseInstance &db);

	void Schedule(std::function<void()> task) override;

	void WaitAll() override;

private:
	TaskExecutor executor;
};

} // namespace duckdb

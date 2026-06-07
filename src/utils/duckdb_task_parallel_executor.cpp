#include "duckdb_task_parallel_executor.hpp"
#include "thread_pool_parallel_executor.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <functional>
#include <utility>

namespace duckdb {

namespace {

class FunctionTask : public BaseExecutorTask {
public:
	FunctionTask(TaskExecutor &executor, std::function<void()> fn) : BaseExecutorTask(executor), fn(std::move(fn)) {
	}

	void ExecuteTask() override {
		fn();
	}

private:
	std::function<void()> fn;
};

} // namespace

DuckDBTaskParallelExecutor::DuckDBTaskParallelExecutor(DatabaseInstance &db)
    : executor(TaskScheduler::GetScheduler(db)) {
}

void DuckDBTaskParallelExecutor::Schedule(std::function<void()> task) {
	executor.ScheduleTask(make_uniq<FunctionTask>(executor, std::move(task)));
}

void DuckDBTaskParallelExecutor::WaitAll() {
	executor.WorkOnTasks();
}

unique_ptr<BaseParallelExecutor> CreateParallelExecutor(optional_ptr<DatabaseInstance> db, ParallelExecutorMode mode,
                                                        size_t thread_count) {
	if (db && mode == ParallelExecutorMode::DUCKDB_TASK_SCHEDULER) {
		return make_uniq<DuckDBTaskParallelExecutor>(*db);
	}
	return make_uniq<ThreadPoolParallelExecutor>(thread_count);
}

} // namespace duckdb

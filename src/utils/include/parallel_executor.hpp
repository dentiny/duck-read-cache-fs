#pragma once

#include "cache_filesystem_config.hpp"

#include <functional>

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;

class BaseParallelExecutor {
public:
	virtual ~BaseParallelExecutor() = default;
	// Disable copy and move.
	BaseParallelExecutor(const BaseParallelExecutor &) = delete;
	BaseParallelExecutor &operator=(const BaseParallelExecutor &) = delete;

	// Enqueue one unit of work.
	virtual void Schedule(std::function<void()> task) = 0;

	// Block until every scheduled task has finished.
	// Re-throws the first exception observed across all tasks.
	virtual void WaitAll() = 0;

protected:
	BaseParallelExecutor() = default;
};

unique_ptr<BaseParallelExecutor> CreateParallelExecutor(optional_ptr<DatabaseInstance> db, ParallelExecutorMode mode,
                                                        size_t thread_count);

} // namespace duckdb

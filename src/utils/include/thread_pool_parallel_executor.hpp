#pragma once

#include "parallel_executor.hpp"
#include "thread_pool.hpp"

#include <functional>
#include <future>

#include "duckdb/common/vector.hpp"

namespace duckdb {

class ThreadPoolParallelExecutor final : public BaseParallelExecutor {
public:
	explicit ThreadPoolParallelExecutor(size_t thread_count);

	void Schedule(std::function<void()> task) override;
	void WaitAll() override;

private:
	ThreadPool pool;
	vector<std::future<void>> futures;
};

} // namespace duckdb

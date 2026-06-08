#include "thread_pool_parallel_executor.hpp"

#include <utility>

namespace duckdb {

ThreadPoolParallelExecutor::ThreadPoolParallelExecutor(size_t thread_count) : pool(thread_count) {
}

void ThreadPoolParallelExecutor::Schedule(std::function<void()> task) {
	futures.emplace_back(pool.Push(std::move(task)));
}

void ThreadPoolParallelExecutor::WaitAll() {
	pool.Wait();
	for (auto &fut : futures) {
		fut.get();
	}
}

} // namespace duckdb

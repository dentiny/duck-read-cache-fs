#pragma once

#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

#include "duckdb/common/queue.hpp"
#include "duckdb/common/vector.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

namespace duckdb {

class ThreadPool {
public:
	ThreadPool();
	explicit ThreadPool(size_t thread_num);

	ThreadPool(const ThreadPool &) = delete;
	ThreadPool &operator=(const ThreadPool &) = delete;

	~ThreadPool() noexcept;

	// @return future for synchronization.
	template <typename Fn, typename... Args>
	auto Push(Fn &&fn, Args &&...args) -> std::future<typename std::result_of_t<Fn(Args...)>>;

	// Block until the threadpool is dead, or all enqueued tasks finish.
	void Wait();

private:
	using Job = std::function<void(void)>;

	size_t idle_num_ DUCKDB_GUARDED_BY(mutex_) = 0;
	bool stopped_ DUCKDB_GUARDED_BY(mutex_) = false;
	concurrency::mutex mutex_;
	std::condition_variable new_job_cv_;
	std::condition_variable job_completion_cv_;
	queue<Job> jobs_ DUCKDB_GUARDED_BY(mutex_);
	vector<std::thread> workers_ DUCKDB_GUARDED_BY(mutex_);
};

template <typename Fn, typename... Args>
auto ThreadPool::Push(Fn &&fn, Args &&...args) -> std::future<typename std::result_of_t<Fn(Args...)>> {
	using Ret = typename std::result_of_t<Fn(Args...)>;

	auto job =
	    std::make_shared<std::packaged_task<Ret()>>(std::bind(std::forward<Fn>(fn), std::forward<Args>(args)...));
	std::future<Ret> result = job->get_future();
	{
		const concurrency::lock_guard<concurrency::mutex> lck(mutex_);
		jobs_.emplace([job = std::move(job)]() mutable { (*job)(); });
		new_job_cv_.notify_one();
	}
	return result;
}

} // namespace duckdb

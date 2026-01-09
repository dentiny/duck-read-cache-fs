#include "thread_pool.hpp"

#include <utility>

#include "duckdb/common/assert.hpp"
#include "thread_utils.hpp"

namespace duckdb {

ThreadPool::ThreadPool() : ThreadPool(GetCpuCoreCount()) {
}

ThreadPool::ThreadPool(size_t thread_num) : idle_num_(thread_num) {
	workers_.reserve(thread_num);
	for (size_t ii = 0; ii < thread_num; ++ii) {
		workers_.emplace_back([this]() {
			for (;;) {
				Job cur_job;
				{
					concurrency::unique_lock<concurrency::mutex> lck(mutex_);
					new_job_cv_.wait(lck, [this]() { return !jobs_.empty() || stopped_; });
					if (stopped_) {
						return;
					}
					cur_job = std::move(jobs_.front());
					jobs_.pop();
					--idle_num_;
				}

				// Execute job out of critical section.
				cur_job();

				{
					concurrency::unique_lock<concurrency::mutex> lck(mutex_);
					++idle_num_;
					job_completion_cv_.notify_one();
				}
			}
		});
	}
}

void ThreadPool::Wait() {
	concurrency::unique_lock<concurrency::mutex> lck(mutex_);
	job_completion_cv_.wait(lck, [this]() {
		if (stopped_) {
			return true;
		}
		if (idle_num_ == workers_.size() && jobs_.empty()) {
			return true;
		}
		return false;
	});
}

ThreadPool::~ThreadPool() noexcept {
	{
		const concurrency::lock_guard<concurrency::mutex> lck(mutex_);
		stopped_ = true;
		new_job_cv_.notify_all();
	}
	for (auto &cur_worker : workers_) {
		D_ASSERT(cur_worker.joinable());
		cur_worker.join();
	}
}

} // namespace duckdb

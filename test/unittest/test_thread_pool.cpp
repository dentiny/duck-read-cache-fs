#include "catch/catch.hpp"

#include <future>

#include "thread_pool.hpp"

namespace {
constexpr int kNumPromise = 10;
void SetPromise(std::promise<void> *promise) {
	promise->set_value();
}
int GetInputValue(int val) {
	return val;
}
} // namespace

using namespace duckdb; // NOLINT

TEST_CASE("Threadpool test", "[threadpool]") {
	// Enqueue lambda.
	{
		std::vector<std::promise<void>> promises(kNumPromise);
		ThreadPool tp(1);
		std::vector<std::future<void>> futures;
		futures.reserve(kNumPromise);
		for (int ii = 0; ii < kNumPromise; ++ii) {
			auto func = [ii, &promises]() mutable {
				promises[ii].set_value();
			};
			futures.emplace_back(tp.Push(std::move(func)));
		}
		tp.Wait();

		for (int ii = 0; ii < kNumPromise; ++ii) {
			promises[ii].get_future().get();
		}
	}

	// Enqueue function with parameters.
	{
		std::vector<std::promise<void>> promises(kNumPromise);
		ThreadPool tp(1);
		std::vector<std::future<void>> futures;
		futures.reserve(kNumPromise);
		for (int ii = 0; ii < kNumPromise; ++ii) {
			futures.emplace_back(tp.Push(SetPromise, &promises[ii]));
		}
		tp.Wait();

		for (int ii = 0; ii < kNumPromise; ++ii) {
			promises[ii].get_future().get();
		}
	}

	// Enqueue function with return value.
	{
		ThreadPool tp(1);
		std::vector<std::future<int>> futures;
		futures.reserve(kNumPromise);
		for (int val = 0; val < kNumPromise; ++val) {
			futures.emplace_back(tp.Push(GetInputValue, val));
		}
		tp.Wait();

		for (int val = 0; val < kNumPromise; ++val) {
			REQUIRE(futures[val].get() == val);
		}
	}
}

#include "thread_utils.hpp"

#if defined(__linux__) || defined(__APPLE__)
#include <pthread.h>
#endif
#include <thread>

#if defined(__linux__)
#include <sched.h>
#endif

namespace duckdb {

void SetThreadName(const std::string &thread_name) {
#if defined(__APPLE__)
	pthread_setname_np(thread_name.c_str());
#elif defined(__linux__)
	// Restricted to 16 characters, include terminator.
	pthread_setname_np(pthread_self(), thread_name.substr(0, 15).c_str());
#endif
}

int GetCpuCoreCount() {
#if defined(__APPLE__)
	return std::thread::hardware_concurrency();
#elif defined(__linux__)
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	sched_getaffinity(0, sizeof(cpuset), &cpuset);
	const int core_count = CPU_COUNT(&cpuset);
	return core_count;
#endif
}

} // namespace duckdb

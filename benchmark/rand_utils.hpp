#pragma once

#include <random>

namespace duckdb {

// Generate a random value base on [min, max) range using in uniform distribution feature.
template <typename T>
T GetRandomValueInRange(T min, T max) {
	thread_local std::random_device rand_dev;
	std::mt19937 rand_engine(rand_dev());
	std::uniform_real_distribution<double> unif(min, max);
	return unif(rand_engine);
}

} // namespace duckdb

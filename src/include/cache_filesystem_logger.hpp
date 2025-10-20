#include "duckdb/logging/file_system_logger.hpp"

namespace duckdb {

// Reference-version macros.
#define DUCKDB_LOG_OPEN_CACHE_HIT(HANDLE)  DUCKDB_LOG_FILE_SYSTEM(HANDLE, "FILE OPEN CACHE HIT");
#define DUCKDB_LOG_OPEN_CACHE_MISS(HANDLE) DUCKDB_LOG_FILE_SYSTEM(HANDLE, "FILE OPEN CACHE MISS");

#define DUCKDB_LOG_READ_CACHE_HIT(HANDLE)  DUCKDB_LOG_FILE_SYSTEM(HANDLE, "FILE READ CACHE HIT");
#define DUCKDB_LOG_READ_CACHE_MISS(HANDLE) DUCKDB_LOG_FILE_SYSTEM(HANDLE, "FILE READ CACHE MISS");

// Pointer-version macros.
#define DUCKDB_LOG_OPEN_CACHE_HIT_PTR(HANDLE)                                                                          \
	do {                                                                                                               \
		if ((HANDLE) != nullptr) {                                                                                     \
			auto &__handle = *HANDLE;                                                                                  \
			DUCKDB_LOG_FILE_SYSTEM(__handle, "FILE OPEN CACHE HIT");                                                   \
		}                                                                                                              \
	} while (0)

#define DUCKDB_LOG_OPEN_CACHE_MISS_PTR(HANDLE)                                                                         \
	do {                                                                                                               \
		if ((HANDLE) != nullptr) {                                                                                     \
			auto &__handle = *HANDLE;                                                                                  \
			DUCKDB_LOG_FILE_SYSTEM(__handle, "FILE OPEN CACHE MISS");                                                  \
		}                                                                                                              \
	} while (0)

#define DUCKDB_LOG_READ_CACHE_HIT_PTR(HANDLE)                                                                          \
	do {                                                                                                               \
		if ((HANDLE) != nullptr) {                                                                                     \
			auto &__handle = *HANDLE;                                                                                  \
			DUCKDB_LOG_FILE_SYSTEM(__handle, "FILE READ CACHE HIT");                                                   \
		}                                                                                                              \
	} while (0)

#define DUCKDB_LOG_READ_CACHE_MISS_PTR(HANDLE)                                                                         \
	do {                                                                                                               \
		if ((HANDLE) != nullptr) {                                                                                     \
			auto &__handle = *HANDLE;                                                                                  \
			DUCKDB_LOG_FILE_SYSTEM(__handle, "FILE READ CACHE MISS");                                                  \
		}                                                                                                              \
	} while (0)

} // namespace duckdb

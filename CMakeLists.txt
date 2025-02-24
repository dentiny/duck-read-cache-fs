cmake_minimum_required(VERSION 3.5)

# TODO(hjiang): Upgrade to higher version.
set(CMAKE_CXX_STANDARD 14)

set(TARGET_NAME read_cache_fs)

# Suppress warnings.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-write-strings")

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/utils/include)
include_directories(src/include)
include_directories(duckdb-httpfs/extension/httpfs/include)
include_directories(duckdb/third_party/httplib)

set(EXTENSION_SOURCES
    src/cache_filesystem.cpp
    src/cache_filesystem_config.cpp
    src/disk_cache_reader.cpp
    src/in_memory_cache_reader.cpp
    src/histogram.cpp
    src/noop_cache_reader.cpp
    src/read_cache_fs_extension.cpp
    src/temp_profile_collector.cpp
    src/utils/filesystem_utils.cpp
    src/utils/thread_pool.cpp
    src/utils/thread_utils.cpp
    duckdb-httpfs/extension/httpfs/create_secret_functions.cpp
    duckdb-httpfs/extension/httpfs/crypto.cpp
    duckdb-httpfs/extension/httpfs/hffs.cpp
    duckdb-httpfs/extension/httpfs/http_state.cpp
    duckdb-httpfs/extension/httpfs/httpfs.cpp
    duckdb-httpfs/extension/httpfs/httpfs_extension.cpp
    duckdb-httpfs/extension/httpfs/s3fs.cpp)

# Avoid building tooling we won't need for release.
set(BUILD_BENCHMARKS
    OFF
    CACHE BOOL "" FORCE)
set(BUILD_FILTERS
    OFF
    CACHE BOOL "" FORCE)
set(BUILD_GENERATORS
    OFF
    CACHE BOOL "" FORCE)
set(BUILD_TESTING
    OFF
    CACHE BOOL "" FORCE)
set(BUILD_FUZZERS
    OFF
    CACHE BOOL "" FORCE)
set(ENABLE_DOCS
    OFF
    CACHE BOOL "" FORCE)
set(ENABLE_TESTING
    OFF
    CACHE BOOL "" FORCE)
set(ENABLE_LINTING
    OFF
    CACHE BOOL "" FORCE)
set(ENABLE_FORMAT
    OFF
    CACHE BOOL "" FORCE)

# Build as shared library.
set(BUILD_SHARED_LIBS ON)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
target_link_libraries(${EXTENSION_NAME} ${OPENSSL_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${OPENSSL_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}")

add_executable(duckdb_read_behavior unit/duckdb_read_behavior.cpp)
target_link_libraries(duckdb_read_behavior ${EXTENSION_NAME})

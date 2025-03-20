cmake_minimum_required(VERSION 3.5)

# TODO(hjiang): Upgrade to higher version.
set(CMAKE_CXX_STANDARD 14)

set(TARGET_NAME cache_httpfs)

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
    src/cache_entry_info.cpp
    src/cache_filesystem.cpp
    src/cache_filesystem_config.cpp
    src/cache_filesystem_ref_registry.cpp
    src/cache_reader_manager.cpp
    src/cache_status_query_function.cpp
    src/disk_cache_reader.cpp
    src/in_memory_cache_reader.cpp
    src/histogram.cpp
    src/noop_cache_reader.cpp
    src/cache_httpfs_extension.cpp
    src/temp_profile_collector.cpp
    src/utils/fake_filesystem.cpp
    src/utils/filesystem_utils.cpp
    src/utils/immutable_buffer.cpp
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

# Test cases.
include_directories(duckdb/third_party/catch)

add_executable(test_noop_cache_reader unit/test_noop_cache_reader.cpp)
target_link_libraries(test_noop_cache_reader ${EXTENSION_NAME})

add_executable(test_disk_cache_filesystem unit/test_disk_cache_filesystem.cpp)
target_link_libraries(test_disk_cache_filesystem ${EXTENSION_NAME})

add_executable(test_in_memory_cache_filesystem
               unit/test_in_memory_cache_filesystem.cpp)
target_link_libraries(test_in_memory_cache_filesystem ${EXTENSION_NAME})

add_executable(test_filesystem_utils unit/test_filesystem_utils.cpp)
target_link_libraries(test_filesystem_utils ${EXTENSION_NAME})

add_executable(test_histogram unit/test_histogram.cpp)
target_link_libraries(test_histogram ${EXTENSION_NAME})

add_executable(test_thread_pool unit/test_thread_pool.cpp)
target_link_libraries(test_thread_pool ${EXTENSION_NAME})

add_executable(test_shared_lru_cache unit/test_shared_lru_cache.cpp)
target_link_libraries(test_shared_lru_cache ${EXTENSION_NAME})

add_executable(test_copiable_value_lru_cache
               unit/test_copiable_value_lru_cache.cpp)
target_link_libraries(test_copiable_value_lru_cache ${EXTENSION_NAME})

add_executable(test_size_literals unit/test_size_literals.cpp)
target_link_libraries(test_size_literals ${EXTENSION_NAME})

add_executable(test_filesystem_config unit/test_filesystem_config.cpp)
target_link_libraries(test_filesystem_config ${EXTENSION_NAME})

add_executable(test_set_extension_config unit/test_set_extension_config.cpp)
target_link_libraries(test_set_extension_config ${EXTENSION_NAME})

add_executable(test_base_cache_filesystem unit/test_base_cache_filesystem)
target_link_libraries(test_base_cache_filesystem ${EXTENSION_NAME})

add_executable(test_string_utils unit/test_string_utils)
target_link_libraries(test_string_utils ${EXTENSION_NAME})

add_executable(test_no_destructor unit/test_no_destructor)
target_link_libraries(test_no_destructor ${EXTENSION_NAME})

# Benchmark
add_executable(read_s3_object benchmark/read_s3_object.cpp)
target_link_libraries(read_s3_object ${EXTENSION_NAME})

add_executable(sequential_read_benchmark
               benchmark/sequential_read_benchmark.cpp)
target_link_libraries(sequential_read_benchmark ${EXTENSION_NAME})

add_executable(random_read_benchmark benchmark/random_read_benchmark.cpp)
target_link_libraries(random_read_benchmark ${EXTENSION_NAME})

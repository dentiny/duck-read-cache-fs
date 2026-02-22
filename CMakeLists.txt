cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

# TODO(hjiang): Upgrade to higher version.
set(CMAKE_CXX_STANDARD 14)

set(TARGET_NAME cache_httpfs)

if(UNIX OR APPLE)
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -Wno-write-strings -Wno-c++17-extensions -Wno-c++20-extensions -Wunused-variable -Wunused-function -Wunused-but-set-variable"
  )
endif()

# Check if compiler is Clang (case-insensitive check)
string(TOLOWER "${CMAKE_CXX_COMPILER_ID}" CMAKE_CXX_COMPILER_ID_LOWER)
string(TOLOWER "${CMAKE_CXX_COMPILER}" CMAKE_CXX_COMPILER_LOWER)
if(CMAKE_CXX_COMPILER_ID_LOWER MATCHES "clang" OR CMAKE_CXX_COMPILER_LOWER
                                                  MATCHES "clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wthread-safety")
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/utils/include)
include_directories(src/include)
include_directories(duckdb-httpfs/src/include)
include_directories(duckdb/third_party/httplib)

# TODO(hjiang): httpfs has wasm client implementation, which is intentionally
# ignored.
set(EXTENSION_SOURCES
    src/base_profile_collector.cpp
    src/cache_entry_info.cpp
    src/cache_exclusion_manager.cpp
    src/cache_exclusion_utils.cpp
    src/cache_httpfs_extension.cpp
    src/cache_httpfs_instance_state.cpp
    src/cache_filesystem.cpp
    src/cache_filesystem_config.cpp
    src/cache_status_query_function.cpp
    src/cache_read_chunk.cpp
    src/disk_cache_reader.cpp
    src/disk_cache_util.cpp
    src/extension_config_query_function.cpp
    src/in_memory_cache_reader.cpp
    src/histogram.cpp
    src/filesystem_status_query_function.cpp
    src/io_operations.cpp
    src/noop_cache_reader.cpp
    src/noop_profile_collector.cpp
    src/temp_profile_collector.cpp
    src/utils/chunk_utils.cpp
    src/utils/fake_filesystem.cpp
    src/utils/filesystem_utils.cpp
    src/utils/mock_filesystem.cpp
    src/utils/scoped_directory.cpp
    src/utils/test_constants.cpp
    src/utils/test_utils.cpp
    src/utils/thread_pool.cpp
    src/utils/thread_utils.cpp
    src/utils/time_utils.cpp
    src/utils/url_utils.cpp
    duckdb-httpfs/src/create_secret_functions.cpp
    duckdb-httpfs/src/crypto.cpp
    duckdb-httpfs/src/hash_functions.cpp
    duckdb-httpfs/src/hffs.cpp
    duckdb-httpfs/src/http_state.cpp
    duckdb-httpfs/src/httpfs.cpp
    duckdb-httpfs/src/httpfs_curl_client.cpp
    duckdb-httpfs/src/httpfs_extension.cpp
    duckdb-httpfs/src/httpfs_httplib_client.cpp
    duckdb-httpfs/src/s3fs.cpp)

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
find_package(CURL REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR})
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto
                      CURL::libcurl)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto
                      CURL::libcurl)

# Link Windows Cryptography API library for certificate loading on Windows
if(MINGW OR WIN32)
  target_link_libraries(${EXTENSION_NAME} crypt32)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} crypt32)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}")

# Test cases.
if(${BUILD_UNITTESTS})
  # Unit tests.
  add_subdirectory(test/unittest)

  # Integration tests.
  include_directories(duckdb/third_party/catch)

  add_executable(test_noop_cache_reader unit/test_noop_cache_reader.cpp)
  target_link_libraries(test_noop_cache_reader ${EXTENSION_NAME})

  add_executable(test_disk_cache_filesystem unit/test_disk_cache_filesystem.cpp)
  target_link_libraries(test_disk_cache_filesystem ${EXTENSION_NAME})

  add_executable(test_disk_cache_validation unit/test_disk_cache_validation.cpp)
  target_link_libraries(test_disk_cache_validation ${EXTENSION_NAME})

  add_executable(test_read_exception_propagation
                 unit/test_read_exception_propagation.cpp)
  target_link_libraries(test_read_exception_propagation ${EXTENSION_NAME})

  add_executable(test_large_file_disk_reader
                 unit/test_large_file_disk_reader.cpp)
  target_link_libraries(test_large_file_disk_reader ${EXTENSION_NAME})

  add_executable(test_disk_cache_with_multi_directories
                 unit/test_disk_cache_with_multi_directories.cpp)
  target_link_libraries(test_disk_cache_with_multi_directories
                        ${EXTENSION_NAME})

  add_executable(test_in_memory_cache_filesystem
                 unit/test_in_memory_cache_filesystem.cpp)
  target_link_libraries(test_in_memory_cache_filesystem ${EXTENSION_NAME})

  add_executable(test_large_file_inmem_reader
                 unit/test_large_file_inmem_reader.cpp)
  target_link_libraries(test_large_file_inmem_reader ${EXTENSION_NAME})

  add_executable(test_max_fanout_subrequest unit/test_max_fanout_subrequest.cpp)
  target_link_libraries(test_max_fanout_subrequest ${EXTENSION_NAME})

  add_executable(test_chunk_utils unit/test_chunk_utils.cpp)
  target_link_libraries(test_chunk_utils ${EXTENSION_NAME})

  add_executable(test_filesystem_utils unit/test_filesystem_utils.cpp)
  target_link_libraries(test_filesystem_utils ${EXTENSION_NAME})

  add_executable(test_cache_filesystem unit/test_cache_filesystem.cpp)
  target_link_libraries(test_cache_filesystem ${EXTENSION_NAME})

  add_executable(test_cache_stats_collection
                 unit/test_cache_stats_collection.cpp)
  target_link_libraries(test_cache_stats_collection ${EXTENSION_NAME})

  add_executable(test_io_operation_latency unit/test_io_operation_latency.cpp)
  target_link_libraries(test_io_operation_latency ${EXTENSION_NAME})

  add_executable(test_filesystem_config unit/test_filesystem_config.cpp)
  target_link_libraries(test_filesystem_config ${EXTENSION_NAME})

  add_executable(test_set_extension_config unit/test_set_extension_config.cpp)
  target_link_libraries(test_set_extension_config ${EXTENSION_NAME})

  add_executable(test_base_cache_filesystem unit/test_base_cache_filesystem.cpp)
  target_link_libraries(test_base_cache_filesystem ${EXTENSION_NAME})

  add_executable(test_cache_filesystem_with_mock
                 unit/test_cache_filesystem_with_mock.cpp)
  target_link_libraries(test_cache_filesystem_with_mock ${EXTENSION_NAME})

  add_executable(test_cache_exclusion_manager
                 unit/test_cache_exclusion_manager.cpp)
  target_link_libraries(test_cache_exclusion_manager ${EXTENSION_NAME})

  add_executable(test_multi_duckdb_instance unit/test_multi_duckdb_instance.cpp)
  target_link_libraries(test_multi_duckdb_instance ${EXTENSION_NAME})
endif()

add_executable(test_clear_cache_on_write_policy
               unit/test_clear_cache_on_write_policy.cpp)
target_link_libraries(test_clear_cache_on_write_policy ${EXTENSION_NAME})

# Benchmark
if(${BUILD_BENCHMARK})
  add_executable(read_s3_object benchmark/read_s3_object.cpp)
  target_link_libraries(read_s3_object ${EXTENSION_NAME})

  add_executable(sequential_read_benchmark
                 benchmark/sequential_read_benchmark.cpp)
  target_link_libraries(sequential_read_benchmark ${EXTENSION_NAME})

  add_executable(random_read_benchmark benchmark/random_read_benchmark.cpp)
  target_link_libraries(random_read_benchmark ${EXTENSION_NAME})
endif()

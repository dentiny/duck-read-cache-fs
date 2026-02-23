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
    src/in_mem_cache_block.cpp
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
    src/utils/hash_utils.cpp
    src/utils/mock_filesystem.cpp
    src/utils/scoped_directory.cpp
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

# Unit tests
add_subdirectory(test/unittest)

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

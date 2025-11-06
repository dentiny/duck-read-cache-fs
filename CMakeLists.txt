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
    src/cache_filesystem.cpp
    src/cache_filesystem_config.cpp
    src/cache_filesystem_ref_registry.cpp
    src/cache_status_query_function.cpp
    src/cache_read_chunk.cpp
    src/cache_reader_manager.cpp
    src/disk_cache_reader.cpp
    src/extension_config_query_function.cpp
    src/in_memory_cache_reader.cpp
    src/histogram.cpp
    src/filesystem_status_query_function.cpp
    src/noop_cache_reader.cpp
    src/temp_profile_collector.cpp
    src/utils/fake_filesystem.cpp
    src/utils/filesystem_utils.cpp
    src/utils/mock_filesystem.cpp
    src/utils/test_utils.cpp
    src/utils/thread_pool.cpp
    src/utils/thread_utils.cpp
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

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}")

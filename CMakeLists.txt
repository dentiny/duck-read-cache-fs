cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME read_cache_fs)

# TODO(hjiang): Add compiler options, include compiler version and warning
# options.
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(src/utils/include)
include_directories(duckdb-httpfs/extension/httpfs/include)
include_directories(duckdb/third_party/httplib)

set(EXTENSION_SOURCES
    src/read_cache_fs_extension.cpp
    src/disk_cache_filesystem.cpp
    src/in_memory_cache_filesystem.cpp
    src/utils/filesystem_utils.cpp
    duckdb-httpfs/extension/httpfs/create_secret_functions.cpp
    duckdb-httpfs/extension/httpfs/crypto.cpp
    duckdb-httpfs/extension/httpfs/hffs.cpp
    duckdb-httpfs/extension/httpfs/http_state.cpp
    duckdb-httpfs/extension/httpfs/httpfs.cpp
    duckdb-httpfs/extension/httpfs/httpfs_extension.cpp
    duckdb-httpfs/extension/httpfs/s3fs.cpp)

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

# C++ examples.
add_executable(read_http_csv example/read_http_csv.cpp)
target_link_libraries(read_http_csv duckdb)

# Test cases.
include_directories(duckdb/third_party/catch)

add_executable(test_disk_cache_filesystem unit/test_disk_cache_filesystem.cpp)
target_link_libraries(test_disk_cache_filesystem ${EXTENSION_NAME})

add_executable(test_in_memory_cache_filesystem
               unit/test_in_memory_cache_filesystem.cpp)
target_link_libraries(test_in_memory_cache_filesystem ${EXTENSION_NAME})

add_executable(test_stale_deletion unit/test_stale_deletion.cpp)
target_link_libraries(test_stale_deletion ${EXTENSION_NAME})

add_executable(test_shared_lru_cache unit/test_shared_lru_cache.cpp)
target_link_libraries(test_shared_lru_cache ${EXTENSION_NAME})

add_executable(test_size_literals unit/test_size_literals.cpp)
target_link_libraries(test_size_literals ${EXTENSION_NAME})

# Benchmark
add_executable(read_s3_object benchmark/read_s3_object.cpp)
target_link_libraries(read_s3_object ${EXTENSION_NAME})

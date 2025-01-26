cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME read_cache_fs)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(duckdb-httpfs/extension/httpfs/include)
include_directories(duckdb/third_party/httplib)

set(EXTENSION_SOURCES
    src/read_cache_fs_extension.cpp
    src/disk_cache_filesystem.cpp
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

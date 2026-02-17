include_directories(include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

file(GLOB_RECURSE CATCHFS_UNITTEST_OBJECTS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_executable(unittest_cache_httpfs main.cpp ${CATCHFS_UNITTEST_OBJECTS})

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS)
  target_link_libraries(
    unittest_cache_httpfs
    test_helpers
    duckdb
    ${EXTENSION_NAME}
    OpenSSL::SSL
    OpenSSL::Crypto
    CURL::libcurl)
else()
  target_link_libraries(
    unittest_cache_httpfs
    test_helpers
    duckdb_static
    ${EXTENSION_NAME}
    OpenSSL::SSL
    OpenSSL::Crypto
    CURL::libcurl)
endif()

if(MINGW OR WIN32)
  target_link_libraries(unittest_cache_httpfs crypt32)
endif()

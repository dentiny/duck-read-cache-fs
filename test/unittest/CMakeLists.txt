include_directories(include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${CMAKE_SOURCE_DIR}/src/utils/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

set(CATCHFS_UNITTEST_OBJECTS
    main.cpp
    test_base_cache_filesystem_from_unit.cpp
    test_cache_exclusion_manager.cpp
    test_cache_filesystem_from_unit.cpp
    test_cache_filesystem_with_mock.cpp
    test_cache_stats_collection_from_unit.cpp
    test_chunk_utils.cpp
    test_clear_cache_on_delete.cpp
    test_clear_cache_on_write.cpp
    test_copiable_value_lru_cache.cpp
    test_counter.cpp
    test_disk_cache_filesystem_from_unit.cpp
    test_disk_cache_util.cpp
    test_disk_cache_validation_from_unit.cpp
    test_disk_cache_with_multi_directories_from_unit.cpp
    test_exclusive_lru_cache.cpp
    test_exclusive_multi_lru_cache.cpp
    test_filesystem_config_from_unit.cpp
    test_filesystem_utils_from_unit.cpp
    test_histogram.cpp
    test_in_memory_cache_filesystem_from_unit.cpp
    test_io_operation_latency.cpp
    test_large_file_disk_reader_from_unit.cpp
    test_large_file_inmem_reader_from_unit.cpp
    test_max_fanout_subrequest.cpp
    test_multi_duckdb_instance_from_unit.cpp
    test_no_destructor.cpp
    test_noop_cache_reader_from_unit.cpp
    test_optional.cpp
    test_oversized_cache_filepath.cpp
    test_read_exception_propagation.cpp
    test_scoped_directory.cpp
    test_set_extension_config_from_unit.cpp
    test_shared_lru_cache.cpp
    test_size_literals.cpp
    test_thread_pool.cpp
    test_url_utils.cpp)

add_executable(unittest_cache_httpfs ${CATCHFS_UNITTEST_OBJECTS})

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

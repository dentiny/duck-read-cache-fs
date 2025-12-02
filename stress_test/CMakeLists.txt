cmake_minimum_required(VERSION 3.14)
project(cache_httpfs_stress_test)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable ASan (to match the DuckDB debug build)
option(ENABLE_SANITIZERS "Enable ASan and UBSan" ON)
if(ENABLE_SANITIZERS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -g")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fno-omit-frame-pointer -g")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

# Find threads
find_package(Threads REQUIRED)

# Path to the parent build directory
set(PARENT_BUILD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../build/debug")
set(DUCKDB_SRC_DIR "${PARENT_BUILD_DIR}/src")

# Include DuckDB headers
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../duckdb/src/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../duckdb/third_party/re2
)

# Link against the DuckDB library from the parent build
find_library(DUCKDB_LIB 
    NAMES duckdb
    PATHS ${DUCKDB_SRC_DIR}
    NO_DEFAULT_PATH
)

if(NOT DUCKDB_LIB)
    message(FATAL_ERROR "DuckDB library not found at ${DUCKDB_SRC_DIR}. Run 'make debug' in the parent directory first.")
endif()

message(STATUS "Found DuckDB library: ${DUCKDB_LIB}")

# Create the stress test executable
add_executable(stress_test main.cpp)

target_link_libraries(stress_test 
    ${DUCKDB_LIB}
    Threads::Threads
    dl
)

# Set RPATH so it can find libduckdb.so at runtime
set_target_properties(stress_test PROPERTIES
    BUILD_RPATH ${DUCKDB_SRC_DIR}
    INSTALL_RPATH ${DUCKDB_SRC_DIR}
)

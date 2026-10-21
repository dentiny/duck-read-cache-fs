# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
#
# Default load config loads cache_httpfs directly, for manual load (to mimic production environment), set [`DONT_LINK`] additionally.
duckdb_extension_load(cache_httpfs
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    EXTENSION_VERSION 0.15.0
    LOAD_TESTS
)
duckdb_extension_statically_link(cache_httpfs)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)

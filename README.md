# duck-read-cache-fs

## Introduction

This repository is made as read-only filesystem for remote access, which serves as cache layer above duckdb [httpfs](https://github.com/duckdb/duckdb-httpfs).

It provides two options:
- In-memory cache, which caches read block in memory, and is only accessible within in single duckdb process;
- On-disk cache, which caches blocks on disk, and could be shared among all duckdb instances.

It's worth notice, cached httpfs extension only supports unix platform due to certain syscalls.

## Build
```sh
CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make <debug>
```

The extension is statically linked into the binary, to execute duckdb with extension enabled you could
```sh
./build/<debug>/duckdb
```
then start using extension
```sql
-- Check all available extensions.
SELECT * FROM duckdb_extensions();
-- Load extension for cached httpfs.
LOAD read_cache_fs;
-- Read remote CSV file.
SELECT * FROM read_csv_auto('https://csvbase.com/meripaterson/stock-exchanges') LIMIT 10;
```

## TODO items
- [ ] Expose a function to clean read cache files on local filesystem
- [ ] Provide options to configure parameters, for example, block size, total memory consumption for in-memory cache, etc

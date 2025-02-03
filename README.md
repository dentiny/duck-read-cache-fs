# duck-read-cache-fs

## Introduction

This repository is made as read-only filesystem for remote access, which serves as cache layer above duckdb [httpfs](https://github.com/duckdb/duckdb-httpfs).

It provides two options:
- In-memory cache, which caches read block in memory, and is only accessible within in single duckdb process;
- On-disk cache, which caches blocks on disk, and could be shared among all duckdb instances.

On-disk cache files are stored under `<cache-directory>/<filename-sha256>.<filename>`, the default cache directory is `/tmp/duckdb_cached_http_cache`.
There're also temporary cache files stored locally for write atomicity, named as `<filename>.<uuid>.httpfs_local_cache.tmp`, also placed under the above cache directory.

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
-- Get all cache file size.
SELECT cache_httpfs_get_cache_size();
-- Clear all local cache files.
SELECT cache_httpfs_clear_cache();
```

## TODO items
- [ ] Clear specification on compilation/link options, for example, C++ version, warning and error options.
- [ ] Provide options to configure parameters, for example, block size, total memory consumption for in-memory cache, etc.
- [ ] Provide an option to profile read cache filesystem and store them in an in-memory duckdb table for later diagnose; key metrics include single IO request latency, request latency distribution, etc.

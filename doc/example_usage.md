### Example usage for cache httpfs.

- Cache httpfs is 100% compatible with native httpfs; we provide an option to fallback to httpfs extension.
```sql
-- Later access won't be cached, but existing cache (whether it's in-memory or on-disk) will be kept.
D SET cache_httpfs_profile_type='noop';
```

- Extension allows user to set on-disk cache file directory, it could be local filesystem or remote storage (via mount), which provides much flexibility.
```sql
D SET cache_httpfs_profile_type='on_disk';
-- By default cache files will be found under `/tmp/duckdb_cache_httpfs_cache`.
D SET cache_httpfs_cache_directory='/tmp/mounted_cache_directory';
```

- For the extension, filesystem requests are split into multiple sub-requests and aligned with block size for parallel IO requests and cache efficiency.
We provide options to tune block size.
```sql
-- By default block size is 64KiB, here we update it to 4KiB.
D SET cache_httpfs_cache_block_size=4096;
```

- Parallel read feature mentioned above is achieved by spawning multiple threads, with users allowed to adjust thread number.
```sql
-- By default we don't set any limit for subrequest number, with the new setting 10 requests will be performed at the same time.
D SET cache_httpfs_max_fanout_subrequest=10;
```

- User could understand IO characteristics by enabling profiling; currently the extension exposes cache access and IO latency distribution.
```sql
D SET cache_httpfs_profile_type='temp';
-- When profiling enabled, dump to local filesystem for better display.
D COPY (SELECT cache_httpfs_get_profile()) TO '/tmp/output.txt';
```

- A rich set of parameters and util functions are provided for the above features, including but not limited to type of caching, IO request size, etc.
Checkout by
```sql
-- Get all extension configs.
D SELECT * FROM duckdb_settings() WHERE name LIKE 'cache_httpfs%';
-- Get all extension util functions.
D SELECT * FROM duckdb_functions() WHERE function_name LIKE 'cache_httpfs%';
```

- Users could clear cache, whether it's in-memory or on-disk with
```sql
D SELECT cache_httpfs_clear_cache();
```
or clear cache for a particular file with
```sql
D SELECT cache_httpfs_clear_cache_for_file('filename');
```
Notice the query could be slow.

- The extension supports not only httpfs, but also ALL filesystems compatible with duckdb.
```sql
D SELECT cache_httpfs_wrap_cache_filesystem('filesystem-name');
```

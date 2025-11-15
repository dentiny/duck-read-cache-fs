### Example usage for cache httpfs.

- Cache httpfs is 100% compatible with native httpfs; we provide an option to fallback to httpfs extension.
```sql
-- Later access won't be cached, but existing cache (whether it's in-memory or on-disk) will be kept.
D SET cache_httpfs_profile_type='noop';
-- To avoid double caching, extension by default disable external file cache;
-- to switch and fallback to default httpfs extension behavior, re-enable external cache.
D SET enable_external_file_cache=true;
```

- Extension allows user to set on-disk cache file directory, it could be local filesystem or remote storage (via mount), which provides much flexibility.
```sql
D SET cache_httpfs_profile_type='on_disk';
-- By default cache files will be found under `/tmp/duckdb_cache_httpfs_cache`.
D SET cache_httpfs_cache_directory='/tmp/mounted_cache_directory';
-- Update min required disk space to enable on-disk cache; by default 5% of disk space is required.
-- Here we set 1GB as the min requried disk size.
D SET cache_httpfs_min_disk_bytes_for_cache=1000000000;
```

- Extension also allows users to configure multiple cache directories, which benefits situations where there're multiple disks mounted at different filesystem locations.
```sql
D SET cache_httpfs_profile_type='on_disk';
-- Specify multiple cache directories, split by `;`; cache blocks will be evenly distributed under specified directories.
-- By default cache files will be found under `/tmp/duckdb_cache_httpfs_cache`.
D SET cache_httpfs_cache_directories_config='/tmp/duckdb_cache_httpfs_cache_1;/tmp/duckdb_cache_httpfs_cache_2';
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

- The extension provides LRU-based bufferpool upon disk cache blocks, so users don't need to access storage unless necessary. It's disabled by default, which turns to leverage page cache.
```sql
D SET cache_httpfs_disk_cache_reader_enable_memory_cache=true;
-- The max bufferpool size is `cache_httpfs_disk_cache_reader_mem_cache_block_count` * `cache_httpfs_cache_block_size`, you can tune it via
D SET cache_httpfs_disk_cache_reader_mem_cache_block_count=2048;
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

- Apart from data block cache, the extension also supports caching other entities, including file handle, file metadata and glob operations. The cache options are turned on by default, users are able to opt off.
```sql
D SET cache_httpfs_enable_metadata_cache=false;
D SET cache_httpfs_enable_glob_cache=false;
D SET cache_httpfs_enable_file_handle_cache=false;

-- Users are able to check cache access information.
-- TODO(hjiang): Update metrics example.
D SELECT * FROM cache_httpfs_cache_access_info_query();

┌─────────────┬─────────────────┬──────────────────┬──────────────────────┐
│ cache_type  │ cache_hit_count │ cache_miss_count │ cache_miss_by_in_use │
│   varchar   │     uint64      │      uint64      │        uint64        │
├─────────────┼─────────────────┼──────────────────┼──────────────────────┤
│ metadata    │               0 │                0 │                    0 │
│ data        │               0 │                0 │                    0 │
│ file handle │               0 │                0 │                    0 │
│ glob        │               0 │                0 │                    0 │
└─────────────┴─────────────────┴──────────────────┴──────────────────────┘
```

- For certain files, applications or users might don't want to cache them. For example, configurations file are usually read and parsed only once. The extension is able to blacklist caching for certain files via exclusion regex.
```sql
D SET cache_httpfs_add_exclusion_regex('.*config.*');
```

- The extension provides table function to list current active configuration, example usage
```sql
D SELECT * FROM cache_httpfs_get_data_cache_config();
┌─────────────────┬──────────────────────────────────┬───────────────────────┬────────────────────────────┬─────────────────────────┐
│ data cache type │      disk cache directories      │ disk cache block size │ disk cache eviction policy │ disk cache memory cache │
│     varchar     │            varchar[]             │        uint64         │          varchar           │         varchar         │
├─────────────────┼──────────────────────────────────┼───────────────────────┼────────────────────────────┼─────────────────────────┤
│ on_disk         │ [/tmp/duckdb_cache_httpfs_cache] │        524288         │ creation_timestamp         │ page-cache              │
└─────────────────┴──────────────────────────────────┴───────────────────────┴────────────────────────────┴─────────────────────────┘
D SELECT * FROM cache_httpfs_get_metadata_cache_config();
┌─────────────────────┬───────────────────────────┐
│ metadata cache type │ metadata cache entry size │
│       varchar       │          uint64           │
├─────────────────────┼───────────────────────────┤
│ enabled             │            250            │
└─────────────────────┴───────────────────────────┘
D SELECT * FROM cache_httpfs_get_file_handle_cache_config();
┌────────────────────────┬──────────────────────────────┐
│ file handle cache type │ file handle cache entry size │
│        varchar         │            uint64            │
├────────────────────────┼──────────────────────────────┤
│ enabled                │             250              │
└────────────────────────┴──────────────────────────────┘
D SELECT * FROM cache_httpfs_get_glob_cache_config();
┌─────────────────┬───────────────────────┐
│ glob cache type │ glob cache entry size │
│     varchar     │        uint64         │
├─────────────────┼───────────────────────┤
│ enabled         │          64           │
└─────────────────┴───────────────────────┘
```

### Consistency specification

All types of filesystem caches provides **eventual consistency** guarantee, which means they don't reflect the latest write result.
Cache entries will become invalid and get evicted after a tunable timeout.

We provide several workarounds, so users are able to configure based on their workload.

#### Users could clear cache based on different cache type.
```sql
-- Clear all types of cache.
D SELECT cache_httpfs_clear_cache();
-- Clear cache based on filepath.
D SELECT cache_httpfs_clear_cache_for_file();
```

> [!CAUTION]
> It's worth noting due to the filepath format for on-disk cache, it's not possible to delete cache entries by filepath (we will try to implement if it's requested).

#### Configure cache timeout, so staleness become more tolerable.
```sql
-- Tune in-memory data block cache timeout.
D SET cache_httpfs_in_mem_cache_block_timeout_millisec=30000;
-- Tune metadata cache timeout.
D SET cache_httpfs_metadata_cache_entry_timeout_millisec=30000;
-- Tune file handle cache timeout.
D SET cache_httpfs_file_handle_cache_entry_timeout_millisec=30000;
-- Tune glob cache timeout.
D SET cache_httpfs_glob_cache_entry_timeout_millisec=30000;
```

#### Disable cache for certain types of cache

Users are able to disable a certain type of cache.
```sql
-- Disable data block cache.
D SET cache_httpfs_type='noop';
-- Disable metdata cache.
D SET cache_httpfs_enable_metadata_cache=false;
-- Disable file handle cache.
D SET cache_httpfs_enable_file_handle_cache=false;
-- Disable glob cache.
D SET cache_httpfs_enable_glob_cache=false;
```

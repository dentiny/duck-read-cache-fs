### Resource consumption

For this extension, users are able to have control of its resource consumption. For each type of cache, we provide certain parameter for users to tune based on their requirement.

```sql
-- Reserve disk space, to avoid on-disk data cache taking too much space.
-- By default the 5% of disk space will be reserved, but it's allowed to override. Eg, the following sql will reserve 5GB space.
D SET cache_httpfs_min_disk_bytes_for_cache=5000000;

-- Control the maximum memory usage for in-memory data cache.
-- The maximum memory consumption is calculated as [cache_httpfs_cache_block_size] * [cache_httpfs_max_in_mem_cache_block_count].
D SET cache_httpfs_max_in_mem_cache_block_count=10;

-- Control the number of metadata cache entries.
D SET cache_httpfs_metadata_cache_entry_size=10;

-- Control the number of file handle entries.
D SET cache_httpfs_file_handle_cache_entry_size=10;

-- Control the number of glob cache entries.
D SET cache_httpfs_glob_cache_entry_size=10;
```

In extreme cases when resource become critically important, users are able to (1) cleanup cache entries; or (2) disable certain cache types.

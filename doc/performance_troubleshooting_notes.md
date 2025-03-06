### Performance troubleshooting notes

cache httpfs extension bakes observability from the beginning of the design: users are able to enable stats for IO operations and dump reports for better display.

For a slow query, it's usually not easy to tell it's CPU bottlenecked or IO bottlenecked, and how much time does your query spend on IO operations, etc.
With cache httpfs extension, one thing you could do to understand the system and query better is:

First, clear the cache.
```sql
D SELECT cache_httpfs_clear_cache();
```
And run the query with profiling enabled.
```sql
D SET cache_httpfs_profile_type='temp';
D <your query>
-- When profiling enabled, dump to local filesystem for better display.
D COPY (SELECT cache_httpfs_get_profile()) TO '/tmp/uncached_io_stats.txt';
```
Now you could check the IO profile results in the file.
It's worth noting that cache httpfs split every request into multiple subrequests in block size, so the IO latency in the record represents a subrequest.

For example, assumne (1) cache filesystem is requested to read from the first byte and read for 8 KiB, and (2) cache block size is 4KiB, filesystem will issue two subrequests, with each request performing a 4KiB read, the latency recorded represents each 4KiB read.

Till this point, we should have a basic understand how IO characteristics look like (i.e. IO latency for a uncached block read).

Second, clear the profile records and perform a cached read with the same query.
```sql
D SELECT cache_httpfs_clear_profile();
-- It should be same query as previous.
D <your query>
-- Now we get a new profile for a cached access.
D COPY (SELECT cache_httpfs_get_profile()) TO '/tmp/cached_io_stats.txt';
```

Third, in theory, as long as the requested file is not too big (so that cache space gets exhausted too quickly), we shouldn't suffer any cache miss.
Now it's good to elimintate the possibility of slow IO operations.

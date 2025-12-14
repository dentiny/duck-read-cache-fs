# 0.11.0

## Fixed

- Allow one single process to run multiple duckdb instances with cache httpfs extension ([#329])

[#329]: https://github.com/dentiny/duck-read-cache-fs/pull/329

## Improved

- Leverage duckdb setting callback to update instance state config active, instead of check all configs passively at every IO operation ([#336])

[#336]: https://github.com/dentiny/duck-read-cache-fs/pull/336

# 0.10.1

## Changed

- Upgrade duckdb, httpfs and extension-ci-tools

# 0.10.0

## Add

- Add option to enable cache entry validation for both in-memory and on-disk cache entries ([#318], [#323])

[#318]: https://github.com/dentiny/duck-read-cache-fs/pull/318
[#323]: https://github.com/dentiny/duck-read-cache-fs/pull/323

## Fixed

- Fix extra empty IO request caused by incorrect chunking calculation ([#323])

[#323]: https://github.com/dentiny/duck-read-cache-fs/pull/323

# 0.9.4

## Added

- Add metrics for bytes to read and bytes to cache ([#306])

[#306]: https://github.com/dentiny/duck-read-cache-fs/pull/306

# 0.9.3

## Changed

- Upgrade duckdb, httpfs and extension-ci-tools ([#304])

[#304]: https://github.com/dentiny/duck-read-cache-fs/pull/304

# 0.9.2

## Fixed

- Fix segfault when `httpfs` extension is already installed and loaded ([#301])

[#301]: https://github.com/dentiny/duck-read-cache-fs/pull/301

# 0.9.1

## Fixed

- Manually load `httpfs` extension, so the extension achieves compatibility with `httpfs` and is able to attach database with remote database files ([#291])

[#291]: https://github.com/dentiny/duck-read-cache-fs/pull/291

# 0.9.0

## Added

- Add a table function to list all cache configurations ([#279])

[#279]: https://github.com/dentiny/duck-read-cache-fs/pull/279

- Add an in-memory cache within disk cache reader ([#280])

[#280]: https://github.com/dentiny/duck-read-cache-fs/pull/280

# 0.8.0

## Added

- Record disk cache read latency ([#268])

[#268]: https://github.com/dentiny/duck-read-cache-fs/pull/268

- Add exclusion regex on filepath to disable cache on certain files ([#275])

[#275]: https://github.com/dentiny/duck-read-cache-fs/pull/275

## Fixed

- Fix httpfs filesystems wrapping ([#266])

[#266]: https://github.com/dentiny/duck-read-cache-fs/pull/266

- Fix cache and file removal ([#272])

[#272]: https://github.com/dentiny/duck-read-cache-fs/pull/272

# 0.7.2

## Added

- Add a SQL function to list all registered filesystems ([#254])

[#254]: https://github.com/dentiny/duck-read-cache-fs/pull/254

## Changed

- Upgrade duckdb, extension-ci and httpfs to latest version

# 0.7.1

## Fixed

- Fix segfault for multi-lru cache ([#250])

[#250]: https://github.com/dentiny/duck-read-cache-fs/pull/250

# 0.7.0

## Changed

- Upgrade support to duckdb v1.4 ([#246])

[#246]: https://github.com/dentiny/duck-read-cache-fs/pull/246

## Improved

- Add local minio and fake GCS to devcontainer for developing and testing purpose ([#237])

[#237]: https://github.com/dentiny/duck-read-cache-fs/pull/237

- Add LRU-based on-disk cache file eviction ([#245])

[#245]: https://github.com/dentiny/duck-read-cache-fs/pull/245

# 0.6.0

## Fixed

- Clean up cache for single file entry should NOT clear all cache entries ([#230])

[#230]: https://github.com/dentiny/duck-read-cache-fs/pull/230

## Changed

- Add last modification timestamp to metadata cache ([#227])

[#227]: https://github.com/dentiny/duck-read-cache-fs/pull/227

- Increase file handle cache size from 125 to 250 ([#234])

[#234]: https://github.com/dentiny/duck-read-cache-fs/pull/234

- Increase metadata cache size from 125 to 250 ([#234])

[#234]: https://github.com/dentiny/duck-read-cache-fs/pull/234

## Improved

- Observability improvement: add cache miss caused by in-use exclusive resource count ([#232])

[#232]: https://github.com/dentiny/duck-read-cache-fs/pull/232

# 0.5.0

## Changed

- Increase IO request size from 64KiB to 512KiB ([#220])

[#220]: https://github.com/dentiny/duck-read-cache-fs/pull/220

- Allow multiple on-disk cache directories ([#221])

[#221]: https://github.com/dentiny/duck-read-cache-fs/pull/221

- Attempt to get file metadata from `OpenFileInfo` ([#223])

[#223]: https://github.com/dentiny/duck-read-cache-fs/pull/223

# 0.4.0

## Changed

- Upgrade duckdb v1.3.2 ([#209])

[#209]: https://github.com/dentiny/duck-read-cache-fs/pull/209

## Fixed

- Fix double caching with external file cache ([#210])

[#210]: https://github.com/dentiny/duck-read-cache-fs/pull/210

# 0.3.0

## Changed

- Upgrade duckdb v1.3.0 and httpfs ([#198])

[#198]: https://github.com/dentiny/duck-read-cache-fs/pull/198

- Re-enable filesystem wrap ([#199])

[#199]: https://github.com/dentiny/duck-read-cache-fs/pull/199

# 0.2.1

## Fixed

- Fix extension compilation with musl libc. ([#174])

[#174]: https://github.com/dentiny/duck-read-cache-fs/pull/174

- Update (aka, revert) duckdb to stable release v1.2.1. ([#176])

[#176]: https://github.com/dentiny/duck-read-cache-fs/pull/176

## Changed

- Temporarily disable filesystem wrap SQL query until a later DuckDB release is available. ([#175])

[#175]: https://github.com/dentiny/duck-read-cache-fs/pull/175

# 0.2.0

## Added

- Allow users to configure min required disk space for disk cache. ([#106])

[#106]: https://github.com/dentiny/duck-read-cache-fs/pull/106

- Cache httpfs extension is able to wrap all duckdb-compatible filesystems. ([#110])

[#110]: https://github.com/dentiny/duck-read-cache-fs/pull/110

- Add cache for file open and glob. ([#133], [#145])

[#133]: https://github.com/dentiny/duck-read-cache-fs/pull/133
[#145]: https://github.com/dentiny/duck-read-cache-fs/pull/145

- Provide SQL function to query cache status. ([#107], [#109])

[#107]: https://github.com/dentiny/duck-read-cache-fs/pull/107
[#109]: https://github.com/dentiny/duck-read-cache-fs/pull/109

- Add stats observability for open and glob operations. ([#126])

[#126]: https://github.com/dentiny/duck-read-cache-fs/pull/126

## Fixed

- Fix data race between open, read and delete on-disk cache files. ([#113])

[#113]: https://github.com/dentiny/duck-read-cache-fs/pull/113

- Fix max thread number for parallel read subrequests. ([#151])

[#151]: https://github.com/dentiny/duck-read-cache-fs/pull/151

- Fix file offset update from httpfs extension upstream change ([#158])

[#158]: https://github.com/dentiny/duck-read-cache-fs/pull/158

## Improved

- Avoid unnecessary string creation for on-disk cache reader. ([#114])

[#114]: https://github.com/dentiny/duck-read-cache-fs/pull/114

## Changed

- Change SQl function to get on-disk cache size from `cache_httpfs_get_cache_size` to `cache_httpfs_get_ondisk_data_cache_size`. ([#153])

[#153]: https://github.com/dentiny/duck-read-cache-fs/pull/153

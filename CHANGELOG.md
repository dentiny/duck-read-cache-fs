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

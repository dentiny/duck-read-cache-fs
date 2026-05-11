---
name: upgrade-duckdb-extension
description: Upgrade the duck-read-cache-fs extension to a new DuckDB release. Use when the user asks to upgrade DuckDB, bump the duckdb submodule, sync to a new DuckDB tag (e.g. v1.5.2), or update the duckdb / duckdb-httpfs / extension-ci-tools submodules together.
---

# Upgrade duck-read-cache-fs to a new DuckDB release

Three submodules must move together. Two are pinned to matching release tags, one tracks `main`. Then build, run two test suites, and write a changelog entry.

## Inputs

Before starting, confirm the target DuckDB version (e.g. `v1.5.2`). Everything else is derived from it.

## Workflow

Track these as a checklist; do not skip ahead:

```
- [ ] 1. Pin duckdb submodule to tags/$TARGET
- [ ] 2. Pin extension-ci-tools submodule to $TARGET (same tag)
- [ ] 3. Pin duckdb-httpfs submodule based on DuckDB repo commit
- [ ] 4. Reconcile CMakeLists.txt EXTENSION_SOURCES with duckdb-httpfs/src/*.cpp
- [ ] 5. Build: CMAKE_BUILD_PARALLEL_LEVEL=10 make reldebug
```

## Reference: historical upgrade commits

- `97359a4` — `Upgrade duckdb v1.5.2`. Minimal: 3 submodules + 1 CMake source line + CHANGELOG.

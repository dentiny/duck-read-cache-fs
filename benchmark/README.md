# Duck-Read-Cache-FS Performance Benchmark Suite

## Overview
This benchmark suite evaluates the performance of our DuckDB fs extension compared to the standard DuckDB httpfs. 
Key aspects measured include:

* Read performance from cloud storage (S3)
* Caching efficiency for frequently accessed data (working)
* Query execution time with cached vs non-cached data (working)
* Memory usage and disk utilization (working)
* Compatibility with existing DuckDB httpfs workflows 

> **Note**
> Our goal is to provide a more efficient, caching-aware alternative to DuckDB's httpfs while maintaining full compatibility.


## Installation
1. Build all release binaries:
```bash
make
```
## Configuration

### AWS Credentials

Set up your AWS credentials in your environment:
```bash
export AWS_ACCESS_KEY_ID='your-key-id'
export AWS_SECRET_ACCESS_KEY='your-secret-key'
export AWS_DEFAULT_REGION='your-region'
```

### Available Benchmark Suites

The compiled benchmarks are located in:
```bash
build/release/extension/read_cache_fs/
```

To list available benchmark suites:
```bash
ls build/release/extension/read_cache_fs/
```

### Executing a Benchmark

Run any benchmark binary from the suite:
```bash
./build/release/extension/read_cache_fs/<benchmark-name>
```

## Benchmark Methodology

### Test Categories

- Read Performance
  - Sequential read operations
  - Random read operations
  - Cache hit ratio analysis


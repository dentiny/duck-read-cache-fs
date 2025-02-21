# Duck-Read-Cache-FS Performance Benchmark Suite

## Overview
This benchmark suite evaluates the performance of our DuckDB fs extension compared to the standard DuckDB httpfs. 
Key aspects measured include:

* Out of box parallel read support ✅
* Read performance from cloud storage ✅
* Compatibility with existing DuckDB httpfs workflows ✅
* Memory usage and disk utilization ✅
* End-to-end query execution time with cached vs. non-cached data (work in progress) 

> **Note**
> Our goal is to provide a more efficient alternative to DuckDB's httpfs extension while maintaining full compatibility. By caching the data locally, you not only get a performance boost but also save network costs from object storage.


## Installation
1. Build all release binaries:
```bash
CMAKE_BUILD_PARALLEL_LEVE=<how-many-thread-you-have>  make 
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

Available benchmark suites:
```bash
build/release/extension/read_cache_fs/read_s3_object
build/release/extension/read_cache_fs/sequential_read_benchmark
build/release/extension/read_cache_fs/random_read_benchmark
```

### Executing a Benchmark

Run any benchmark binary from the suite:
```bash
./build/release/extension/read_cache_fs/<benchmark-name>
```

## Benchmark Methodology
### Environment Setup

#### Location Details
- **Benchmark Region**: us-west1
- **S3 Storage Bucket Location**: ap-northeast-1

#### Hardware Specifications

**CPU Architecture**
- Architecture: x86_64
- Operation Modes: 32-bit, 64-bit
- Physical/Virtual Address: 46 bits physical, 48 bits virtual
- Byte Order: Little Endian
- Total CPUs: 32 (all online, cores 0-31)

**Cache Information**
- L1 Data Cache: 512 KiB (16 instances)
- L1 Instruction Cache: 512 KiB (16 instances)
- L2 Cache: 16 MiB (16 instances)
- L3 Cache: 35.8 MiB (1 instance)

**Memory Configuration**
- Range 1: 0x0000000000000000-0x00000000bfffffff
  - Size: 3G
  - State: Online
  - Removable: Yes
  - Blocks: 0-23

- Range 2: 0x0000000100000000-0x0000001fe7ffffff
  - Size: 123.6G
  - State: Online
  - Removable: Yes
  - Blocks: 32-1020

### Test Categories

- Read Performance
  - Sequential read operations
  - Random read operations


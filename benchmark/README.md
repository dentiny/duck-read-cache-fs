# Duck-Read-Cache-FS Performance Benchmark Suite

## Overview
This benchmark suite evaluates the performance of our DuckDB fs extension compared to the standard DuckDB httpfs. 

## Configuration

### AWS Credentials
Set up your AWS credentials in your environment:
```bash
export AWS_ACCESS_KEY_ID='your-key-id'
export AWS_SECRET_ACCESS_KEY='your-secret-key'
export AWS_DEFAULT_REGION='your-region'
```

### Available Benchmark Suites
Available benchmark suites:
```bash
build/release/extension/read_cache_fs/read_s3_object
build/release/extension/read_cache_fs/sequential_read_benchmark
build/release/extension/read_cache_fs/random_read_benchmark
```

## Benchmark Methodology
### Environment Setup

#### Location Details
- **Benchmark Machine Region**: us-west1
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
  - [Sequential read operations](benchmark/sequential_read_benchmark.cpp)
  - [Random read operations](benchmark/random_read_benchmark.cpp)


# Duck-Read-Cache-FS Performance Benchmark Suite

## Overview
This benchmark suite evaluates the performance of our DuckDB fs extension compared to the standard DuckDB httpfs. 

The TLDR here is:
- If request size larger than request block size, our extension's performance is much better than httpfs due to parallelism and cache hit
- If request size smaller than request block size, its performance is similar to httpfs but slightly worse, because extension read more bytes due to alignment
- Overall, the extension provides no worse performance, meanwhile providing a few extra features

## Configuration

### AWS Credentials
Set up your AWS credentials in your environment:
```bash
export AWS_ACCESS_KEY_ID='your-key-id'
export AWS_SECRET_ACCESS_KEY='your-secret-key'
export AWS_DEFAULT_REGION='your-region'
```

### Available Benchmark Suites
```bash
build/release/extension/cache_httpfs/read_s3_object
build/release/extension/cache_httpfs/sequential_read_benchmark
build/release/extension/cache_httpfs/random_read_benchmark
```

## Benchmark Methodology

### Environment Setup

#### Location Details
- Benchmark Machine Region: us-west1
- S3 Storage Bucket Location: ap-northeast-1

#### Hardware Specifications

```sh
ubuntu$ lscpu
Architecture:            x86_64
  CPU op-mode(s):        32-bit, 64-bit
  Address sizes:         46 bits physical, 48 bits virtual
  Byte Order:            Little Endian
CPU(s):                  32
  On-line CPU(s) list:   0-31
Caches (sum of all):     
  L1d:                   512 KiB (16 instances)
  L1i:                   512 KiB (16 instances)
  L2:                    16 MiB (16 instances)
  L3:                    35.8 MiB (1 instance)

ubuntu$ lsmem 
RANGE                                   SIZE  STATE REMOVABLE   BLOCK
0x0000000000000000-0x00000000bfffffff     3G online       yes    0-23
0x0000000100000000-0x0000001fe7ffffff 123.6G online       yes 32-1020

Memory block size:       128M
Total online memory:    126.6G
Total offline memory:      0B
```

### Test Categories

- [Sequential read operations](https://github.com/dentiny/duck-read-cache-fs/blob/main/benchmark/random_read_benchmark.cpp)
- [Random read operations](https://github.com/dentiny/duck-read-cache-fs/blob/main/benchmark/random_read_benchmark.cpp)

# Cache HTTPFS Stress Test

A multi-threaded stress test to reproduce race conditions in the cache_httpfs extension.

## Prerequisites

1. Build the parent project with debug symbols:
   ```bash
   cd ..
   make debug
   ```

2. Start MinIO:
   ```bash
   cd ..
   docker-compose up -d
   ```

## Building

```bash
./build.sh
```

## Running

### Simple run:
```bash
./run_stress_test.sh
```

### With custom parameters:
```bash
./run_stress_test.sh [num_threads] [iterations_per_thread]

# Examples:
./run_stress_test.sh 2 3    # 2 threads, 3 iterations each (default)
./run_stress_test.sh 4 10   # 4 threads, 10 iterations each (more aggressive)
./run_stress_test.sh 8 20   # 8 threads, 20 iterations each (very aggressive)
```

### Manual run with GDB:
```bash
cd build
gdb ./stress_test
(gdb) run 2 3
```

### Run with ASan output:
```bash
cd build
ASAN_OPTIONS="detect_leaks=0:halt_on_error=1:verbosity=1" ./stress_test 4 5
```

## What it does

Each thread:
1. Creates its own DuckDB instance
2. Loads cache_httpfs and ducklake extensions
3. Configures S3 credentials for MinIO
4. Creates a DuckLake attached database with a unique S3 path
5. Generates TPCH data and writes it to DuckLake
6. Performs multiple read operations to exercise the cache:
   - Sequential reads (cache miss then hit)
   - Different column reads
   - Join queries

## Expected behavior

The test should complete without crashes. If you see:
- Segmentation faults
- ASan errors (heap-use-after-free, etc.)
- ThreadSanitizer warnings

...then we've found the bug!

## Tuning for reproduction

If the bug doesn't reproduce easily, try:
1. Increase threads: `./run_stress_test.sh 8 10`
2. Reduce block size to increase cache churn
3. Run in a loop: `while ./run_stress_test.sh 4 5; do echo "Pass"; done`


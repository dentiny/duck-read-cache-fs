#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NUM_THREADS=${1:-8}
ITERATIONS=${2:-4}

echo "=== Cache HTTPFS Stress Test Runner ==="
echo ""

# Check if MinIO is running
if ! docker ps | grep -q minio-ducklake; then
    echo "Starting MinIO..."
    cd ..
    docker-compose up -d
    sleep 3
    cd "$SCRIPT_DIR"
else
    echo "MinIO is already running"
fi

# Build if necessary
if [ ! -f "build/stress_test" ]; then
    echo "Building stress test..."
    ./build.sh
fi

echo ""
echo "Running stress test with $NUM_THREADS threads and $ITERATIONS iterations each..."
echo ""

# Run with ASan options for better error reporting
export ASAN_OPTIONS="detect_leaks=0:halt_on_error=1:print_stats=1:detect_stack_use_after_return=1"
export UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1"

cd build
./stress_test $NUM_THREADS $ITERATIONS

echo ""
echo "=== Test Complete ==="


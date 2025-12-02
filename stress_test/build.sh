#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if parent build exists
if [ ! -f "../build/debug/libduckdb.so" ]; then
    echo "DuckDB debug build not found. Building parent project first..."
    cd ..
    make debug
    cd "$SCRIPT_DIR"
fi

# Create build directory
mkdir -p build
cd build

# Configure and build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j$(nproc)

echo ""
echo "Build complete! Run with:"
echo "  cd build && ./stress_test [num_threads] [iterations_per_thread]"
echo ""
echo "Example:"
echo "  ./stress_test 4 5    # 4 threads, 5 iterations each"


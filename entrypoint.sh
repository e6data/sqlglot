#!/bin/sh

# Detect number of CPU cores
CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "2")

# Calculate optimal workers: (2 × cores) + 1
# Min: 2, Max: 20 (safety limits)
OPTIMAL_WORKERS=$((CPU_CORES * 2 + 1))

# Apply limits
if [ $OPTIMAL_WORKERS -lt 2 ]; then
    OPTIMAL_WORKERS=2
elif [ $OPTIMAL_WORKERS -gt 20 ]; then
    OPTIMAL_WORKERS=20
fi

# Allow environment variable override
WORKERS=${UVICORN_WORKERS:-$OPTIMAL_WORKERS}

echo "System has $CPU_CORES CPU cores"
echo "Starting with $WORKERS workers"

# Start the application
exec uvicorn converter_api:app \
    --host 0.0.0.0 \
    --port 8100 \
    --workers $WORKERS \
    --proxy-headers
#!/bin/bash

echo "Setting up distributed processing environment..."

# Install Redis if not installed (macOS)
if ! command -v redis-server &> /dev/null; then
    echo "Installing Redis..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install redis
    else
        echo "Please install Redis manually for your system"
        exit 1
    fi
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Start Redis if not running
if ! pgrep -x "redis-server" > /dev/null; then
    echo "Starting Redis server..."
    redis-server --daemonize yes
else
    echo "Redis is already running"
fi

echo "Setup complete!"
echo ""
echo "To start processing:"
echo "1. In terminal 1: ./start_workers.sh"
echo "2. In terminal 2: python orchestrator.py <s3_path> [query_column]"
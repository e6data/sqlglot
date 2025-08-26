#!/bin/bash

# Start Celery Worker for Automated Processing
# Following TestDriven.io FastAPI+Celery patterns

echo "🚀 Starting Celery worker for automated SQL processing..."
echo "📁 Working directory: $(pwd)"
echo ""

# Change to the automated_processing directory
cd "$(dirname "$0")"

# Check if Redis is running and start if needed
if ! redis-cli ping > /dev/null 2>&1; then
    echo "⚠️  Redis is not running. Starting Redis..."
    
    # Try to start Redis with brew services first
    if command -v brew > /dev/null 2>&1; then
        echo "🔧 Starting Redis with brew services..."
        brew services start redis
        sleep 2  # Give Redis time to start
        
        # Check if Redis is now running
        if redis-cli ping > /dev/null 2>&1; then
            echo "✅ Redis started successfully with brew"
        else
            echo "❌ Failed to start Redis with brew services"
            echo "   Try manually: brew services start redis"
            exit 1
        fi
    else
        # Try to start Redis directly
        echo "🔧 Starting Redis server directly..."
        redis-server --daemonize yes
        sleep 2  # Give Redis time to start
        
        # Check if Redis is now running
        if redis-cli ping > /dev/null 2>&1; then
            echo "✅ Redis started successfully"
        else
            echo "❌ Failed to start Redis"
            echo "   Please install Redis or start it manually"
            exit 1
        fi
    fi
else
    echo "✅ Redis is already running"
fi

# Start the Celery worker
echo "🔧 Starting Celery worker..."
echo "   Command: celery -A worker.celery worker --loglevel=info"
echo ""

# Use prefork pool with s3fs for multiprocessing performance without SIGSEGV
# s3fs is more multiprocessing-friendly than PyArrow S3FileSystem
celery -A worker.celery worker --loglevel=info --pool=prefork --autoscale=8,1 -Q processing_queue

echo ""
echo "👋 Worker stopped"
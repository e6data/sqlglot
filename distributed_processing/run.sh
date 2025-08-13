#!/bin/bash

# Script to run the distributed processing setup

# Check command
if [ "$1" == "worker" ]; then
    echo "Starting Celery workers..."
    cd /Users/niranjgaurav/PycharmProjects/sqlglot/distributed_processing
    
    # Start Celery worker with 4 concurrent processes
    celery -A celery_worker worker \
        --loglevel=info \
        --concurrency=4 \
        --queues=parquet_processing

elif [ "$1" == "process" ]; then
    if [ -z "$2" ]; then
        echo "Usage: ./run.sh process <s3_path> [query_column] [from_sql] [to_sql]"
        exit 1
    fi
    
    echo "Starting orchestrator..."
    cd /Users/niranjgaurav/PycharmProjects/sqlglot/distributed_processing
    python orchestrator.py "$2" "$3" "$4" "$5"

elif [ "$1" == "test" ]; then
    echo "Running test with sample S3 path..."
    cd /Users/niranjgaurav/PycharmProjects/sqlglot/distributed_processing
    
    # Test with a sample S3 path
    python -c "
from orchestrator import ParquetOrchestrator
import json

orchestrator = ParquetOrchestrator()

# Test validation
result = orchestrator.validate_s3_bucket('s3://your-bucket/path/to/file.parquet')
print('Validation result:')
print(json.dumps(result, indent=2))
"

elif [ "$1" == "status" ]; then
    if [ -z "$2" ]; then
        echo "Usage: ./run.sh status <session_id>"
        exit 1
    fi
    
    echo "Checking session status..."
    cd /Users/niranjgaurav/PycharmProjects/sqlglot/distributed_processing
    python -c "
from redis_manager import RedisJobManager
import json

manager = RedisJobManager()
status = manager.get_session_status('$2')
print(json.dumps(status, indent=2))
"

else
    echo "Distributed Parquet Processing Runner"
    echo "======================================"
    echo ""
    echo "Usage:"
    echo "  ./run.sh worker                    - Start Celery workers"
    echo "  ./run.sh process <s3_path> [args]  - Process S3 parquet files"
    echo "  ./run.sh test                      - Test the setup"
    echo "  ./run.sh status <session_id>       - Check session status"
    echo ""
    echo "Example:"
    echo "  Terminal 1: ./run.sh worker"
    echo "  Terminal 2: ./run.sh process s3://bucket/file.parquet query_column snowflake e6"
fi
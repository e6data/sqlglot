# Batch Processing Architecture for SQL Transpilation

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture Design](#architecture-design)
3. [Core Components](#core-components)
4. [Processing Flow](#processing-flow)
5. [Data Structures](#data-structures)
6. [Implementation Details](#implementation-details)
7. [Failure Recovery](#failure-recovery)
8. [Performance Optimization](#performance-optimization)
9. [Example Workflow](#example-workflow)

---

## System Overview

This document describes a distributed batch processing system for transpiling large volumes of SQL queries from parquet files. The system uses hash-based batching with Redis and Celery for distributed task processing.

### Key Features
- **Single API entry point** for processing entire directories
- **Hash-based batch dictionaries** for query organization
- **Distributed processing** using Celery workers
- **Automatic failure recovery** at batch level
- **Real-time progress tracking** via Redis

### Architecture Goals
- Process 500k-600k queries efficiently
- Handle large-scale query processing with optimal batching
- Enable horizontal scaling
- Provide atomic batch-level recovery
- Maintain simplicity in API interface

---

## Architecture Design

### High-Level Architecture
```
Frontend Application
        ↓
    Single API Endpoint (/process-parquet-directory)
        ↓
    Pre-scanner (Counts unique queries per file)
        ↓
    Task Distributor (Creates modulo tasks)
        ↓
    Redis (Stores only task metadata)
        ↓
    Celery Workers (Read files & create batches on-the-fly)
        ↓
    Iceberg Warehouse (Stores results)
```

### Component Interaction
```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Frontend  │────▶│     API     │────▶│ Pre-scanner │
│             │     │   Endpoint  │     │ (unique cnt)│
└─────────────┘     └─────────────┘     └─────────────┘
                            │                    │
                            ▼                    ▼
                    ┌─────────────┐     ┌─────────────┐
                    │   Session   │     │    Task     │
                    │   Manager   │     │ Distributor │
                    └─────────────┘     └─────────────┘
                            │                    │
                            ▼                    ▼
                    ┌─────────────┐     ┌─────────────┐
                    │   Progress  │◀────│    Redis    │
                    │   Tracker   │     │ (task meta) │
                    └─────────────┘     └─────────────┘
                                                │
                                                ▼
                                        ┌─────────────┐
                                        │   Celery    │
                                        │   Workers   │
                                        │ (read files)│
                                        └─────────────┘
                                                │
                                                ▼
                                        ┌─────────────┐
                                        │   Iceberg   │
                                        │  Warehouse  │
                                        └─────────────┘
```

---

## Core Components

### 1. API Endpoint
**Path:** `/process-parquet-directory`

**Purpose:** Single entry point for batch processing entire directories of parquet files

**Request Format:**
```json
{
    "directory_path": "/path/to/parquet/files",
    "from_dialect": "databricks",
    "to_dialect": "e6",
    "batch_size": 10000,
    "use_batch_processing": true
}
```

**Response Format:**
```json
{
    "session_id": "session_abc123",
    "total_files": 3,
    "total_queries": 557080,
    "unique_queries": 297478,
    "pre_scan_time_seconds": 2.1,
    "total_batches": 30,
    "estimated_time_minutes": 45,
    "status": "processing",
    "tracking_url": "/status/session_abc123"
}
```

### 2. Task Distribution Module
**Purpose:** Pre-scans files for unique count and distributes optimal modulo tasks

**Key Functions:**
- `calculate_optimal_batches(file_path, target_batch_size)` - Scans for unique queries
- `submit_modulo_tasks(file_path, num_batches)` - Submits tasks to Celery

**Task Distribution Strategy:**
```python
# Step 1: Pre-scan to determine optimal batch count
def calculate_optimal_batches(file_path, target_batch_size=10000):
    # Fast scan - read only hash column
    df_hashes = pd.read_parquet(file_path, columns=['query_hash'])
    unique_count = df_hashes['query_hash'].nunique()
    
    # Calculate batches based on unique queries
    num_batches = max(1, unique_count // target_batch_size)
    
    return num_batches, unique_count

# Step 2: Submit modulo tasks
num_batches, unique_count = calculate_optimal_batches(file_path)
for i in range(num_batches):
    task = {
        "file_path": "/data/queries.parquet",
        "remainder": i,
        "total_batches": num_batches,
        "estimated_unique_per_batch": unique_count // num_batches
    }
    celery.send_task("process_modulo_batch", args=[task])

# Worker filters queries based on modulo
# Processes all queries where: hash(query_hash) % total_batches == remainder
```

### 3. Redis Storage Manager
**Purpose:** Tracks task status and session progress (no batch data storage)

**Key Storage Patterns:**
```
# Task metadata only (no query data)
task:{task_id}:meta → {
    "file_path": "file1.parquet",
    "remainder": 0,
    "total_batches": 50,
    "status": "pending|processing|completed|failed",
    "retry_count": 0,
    "worker_id": null,
    "created_at": "2024-01-15T10:30:00"
}

# Session tracking
session:{session_id}:info → {
    "directory": "/data/parquet/",
    "total_tasks": 50,
    "completed_tasks": 23,
    "failed_tasks": 2,
    "start_time": "2024-01-15T10:30:00"
}

session:{session_id}:tasks → ["task_001", "task_002", ...]
session:{session_id}:pending → Set of pending task IDs
session:{session_id}:completed → Set of completed task IDs
session:{session_id}:failed → Set of failed task IDs
```

### 4. Celery Worker
**Purpose:** Creates and processes batches based on modulo assignment

**Task Definition:**
```python
@celery_app.task(bind=True, max_retries=3)
def process_modulo_batch(self, file_path, remainder, total_batches, from_dialect, to_dialect):
    """
    Process queries matching modulo condition
    
    Steps:
    1. Read parquet file (streaming for large files)
    2. Filter queries where hash(query_hash) % total_batches == remainder
    3. Create batch dictionary on-the-fly
    4. Transpile all matching queries using SQLGlot
    5. Store results in Iceberg
    6. Update task status in Redis
    """
    
    # Worker creates its own batch
    batch_dict = {}
    for chunk in pd.read_parquet(file_path, chunksize=50000):
        for row in chunk.itertuples():
            if hash(row.query_hash) % total_batches == remainder:
                batch_dict[row.query_hash] = row.hashed_query
    
    # Process the batch
    results = transpile_batch(batch_dict, from_dialect, to_dialect)
    store_to_iceberg(results)
```

### 5. Progress Tracker
**Purpose:** Monitors and reports processing progress

**Endpoints:**
- `GET /status/{session_id}` - Get current processing status
- `GET /batch/{batch_id}/status` - Get specific batch status
- `POST /retry/{batch_id}` - Retry failed batch

---

## Processing Flow

### Complete Processing Pipeline

```
1. API receives directory path
   │
   ├─▶ 2. Scan directory for parquet files
   │      Example: [file1.parquet, file2.parquet, file3.parquet]
   │
   ├─▶ 3. For each parquet file:
   │      a. Pre-scan file to count unique query_hash values
   │      b. Calculate optimal number of batches (unique_count / target_batch_size)
   │      c. Submit modulo tasks to Celery queue
   │      d. Store task metadata in Redis
   │
   ├─▶ 4. Return session ID to frontend
   │
   └─▶ 5. Workers process batches in parallel:
          a. Pull task (file_path, remainder, total_batches) from queue
          b. Read file and filter by modulo condition
          c. Create batch dictionary on-the-fly
          d. Transpile queries
          e. Store results
          f. Update status
```

### Detailed Batch Processing

```python
# Step 1: Task Distribution (in API/Orchestrator)
def distribute_tasks(file_path, num_batches=50):
    tasks = []
    for i in range(num_batches):
        task = {
            "file_path": file_path,
            "remainder": i,
            "total_batches": num_batches
        }
        task_id = f"task_{uuid.uuid4().hex[:8]}"
        
        # Store only task metadata in Redis
        redis.hset(f"task:{task_id}:meta", mapping=task)
        
        # Submit to Celery
        celery.send_task("process_modulo_batch", args=[task])
        tasks.append(task_id)
    
    return tasks

# Step 2: Worker Processing (creates batch on-the-fly)
def process_modulo_batch(file_path, remainder, total_batches):
    batch_dict = {}
    
    # Stream file and filter by modulo
    for chunk in pd.read_parquet(file_path, chunksize=50000):
        for row in chunk.itertuples():
            if hash(row.query_hash) % total_batches == remainder:
                batch_dict[row.query_hash] = row.hashed_query
    
    # Process the batch
    results = []
    for query_hash, query_text in batch_dict.items():
        converted = sqlglot.transpile(query_text, from_dialect, to_dialect)
        results.append({
            "query_hash": query_hash,
            "original": query_text,
            "converted": converted
        })
    
    store_to_iceberg(results)
    update_task_status(task_id, "completed")
```

---

## Data Structures

### Task Structure (No Stored Batches)
```json
{
    "task_id": "task_a1b2c3d4",
    "file_path": "/data/queries_2024_01.parquet",
    "remainder": 0,
    "total_batches": 30,
    "estimated_unique_per_batch": 9916,
    "status": "pending",
    "created_timestamp": "2024-01-15T10:30:00Z",
    "description": "Process queries where hash(query_hash) % 30 == 0"
}
```

**Batch Count Calculation:**
- Pre-scan determines unique query count (e.g., 297,478 unique queries)
- Target batch size: 10,000 unique queries per batch
- Calculated batches: 297,478 ÷ 10,000 = 30 batches
- Each worker processes ~9,916 unique queries

**Note:** Workers create batch dictionaries on-the-fly by filtering the parquet file. No batch data is stored in Redis or elsewhere.

### Session Tracking Structure
```json
{
    "session_id": "session_xyz789",
    "directory_path": "/data/parquet/2024/",
    "files_processed": [
        {
            "filename": "queries_jan.parquet",
            "total_rows": 100000,
            "unique_queries": 65000,
            "pre_scan_time_seconds": 2.5,
            "batches_created": 7,
            "target_batch_size": 10000,
            "status": "completed"
        },
        {
            "filename": "queries_feb.parquet",
            "total_rows": 200000,
            "unique_queries": 120000,
            "pre_scan_time_seconds": 3.8,
            "batches_created": 12,
            "target_batch_size": 10000,
            "status": "processing"
        }
    ],
    "progress": {
        "total_batches": 19,
        "completed": 12,
        "processing": 2,
        "pending": 4,
        "failed": 1
    },
    "performance": {
        "start_time": "2024-01-15T10:30:00Z",
        "elapsed_seconds": 1200,
        "unique_queries_per_second": 385,
        "estimated_completion": "2024-01-15T11:15:00Z"
    }
}
```

---

## Implementation Details

### File Processing Strategy

#### For Small Files (< 100MB)
```python
# Direct processing
df = pd.read_parquet(file_path)
create_batches_from_dataframe(df)
```

#### For Large Files (> 100MB)
```python
# Streaming approach
for chunk in pd.read_parquet(file_path, chunksize=50000):
    process_chunk(chunk)
```

### Pre-scan Strategy for Optimal Batching

```python
def calculate_optimal_batches(file_path, target_batch_size=10000):
    """
    Pre-scan file to count unique queries and determine optimal batch count.
    This scan is fast since we only read the query_hash column.
    """
    start_time = time.time()
    
    # Fast scan - only read hash column (much faster than full file)
    df_hashes = pd.read_parquet(file_path, columns=['query_hash'])
    unique_count = df_hashes['query_hash'].nunique()
    
    scan_time = time.time() - start_time
    
    # Calculate optimal batches based on unique count
    num_batches = max(1, unique_count // target_batch_size)
    
    return {
        "num_batches": num_batches,
        "unique_count": unique_count,
        "scan_time_seconds": scan_time,
        "queries_per_batch": unique_count // num_batches
    }

def create_modulo_batch(file_path, remainder, total_batches):
    """
    Worker creates its own batch based on modulo assignment.
    No pre-created batches, no storage overhead.
    """
    batch_dict = {}
    
    # Stream file to save memory
    for chunk in pd.read_parquet(file_path, chunksize=50000):
        for row in chunk.itertuples():
            # Check if this query belongs to this worker's batch
            if hash(row.query_hash) % total_batches == remainder:
                batch_dict[row.query_hash] = row.hashed_query
    
    # Batch dictionary exists only in worker memory
    return batch_dict
```

**Benefits:**
- **Optimal batch sizing** based on actual unique query count
- **Fast pre-scan** (only reads hash column, not full queries)
- **No storage overhead** in Redis
- **Workers are completely independent**
- **Perfect recovery** - just re-run the same modulo filter
- **Predictable batch sizes** (~10k unique queries per batch)

### Memory Management

```python
# Batch size calculation based on available memory
def calculate_optimal_batch_size():
    available_memory = get_available_memory()
    avg_query_size = 2000  # bytes
    overhead_factor = 1.5
    
    max_queries = available_memory / (avg_query_size * overhead_factor)
    
    # Use standard batch sizes
    if max_queries > 20000:
        return 15000
    elif max_queries > 10000:
        return 10000
    else:
        return 5000
```

---

## Failure Recovery

### Batch-Level Recovery

```python
# Automatic retry mechanism
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_batch_with_retry(self, batch_id):
    try:
        process_batch_dictionary(batch_id)
    except Exception as exc:
        # Update retry count in Redis
        redis.hincrby(f"batch:{batch_id}:meta", "retry_count", 1)
        
        # Exponential backoff
        retry_delay = 60 * (2 ** self.request.retries)
        
        # Re-raise for Celery retry
        raise self.retry(exc=exc, countdown=retry_delay)
```

### Manual Recovery Options

```python
# Retry specific batch
POST /retry/batch/{batch_id}

# Retry all failed batches in session
POST /retry/session/{session_id}/failed

# Reset batch status for reprocessing
POST /reset/batch/{batch_id}
```

### Recovery Strategies

1. **Automatic Retry**: Failed batches retry up to 3 times with exponential backoff
2. **Dead Letter Queue**: Batches failing 3+ times moved to DLQ for manual inspection
3. **Partial Recovery**: Can retry individual queries within a failed batch
4. **Session Recovery**: Entire session can be restarted from last checkpoint

---

## Performance Optimization

### Optimization Techniques

#### 1. Efficient Batch Processing
- **Impact**: Process only unique queries (dictionary structure)
- **Implementation**: Hash-based dictionary with query_hash as key

#### 2. Batch Size Tuning
```python
# Optimal batch sizes for different scenarios
BATCH_SIZE_CONFIG = {
    "small_queries": 15000,    # < 500 chars per query
    "medium_queries": 10000,    # 500-2000 chars
    "large_queries": 5000,      # > 2000 chars
    "complex_queries": 2000     # With many joins/subqueries
}
```

#### 3. Parallel Processing
- **Worker Scaling**: `workers = min(num_batches / 10, max_workers)`
- **Concurrency**: 4-8 workers per machine optimal
- **Prefetch**: Set to 1 to prevent task hoarding

#### 4. Memory Optimization
```python
# Clear batch from Redis after processing
def cleanup_completed_batch(batch_id):
    redis.delete(f"batch:{batch_id}:data")
    # Keep metadata for tracking
    redis.expire(f"batch:{batch_id}:meta", 3600)  # 1 hour TTL
```

### Performance Metrics

| Metric | Target | Actual |
|--------|--------|--------|
| Queries per second | 500 | 450-550 |
| Batch processing time | < 2 min | 1.5-2.5 min |
| Memory per batch | < 100MB | 80-120MB |
| Worker utilization | > 80% | 85-90% |
| Processing reduction | 40% | 35-45% |

---

## Example Workflow

### Processing 500k Query File

```
Input: /data/queries-hashed.snappy.parquet
- Total rows: 557,080
- Unique queries: 297,478
- File size: 1.7GB

Step 1: API Call
POST /process-parquet-directory
{
    "directory_path": "/data/",
    "batch_size": 10000
}

Step 2: Pre-scan and Task Distribution
- Pre-scan finds 297,478 unique queries (2.1 seconds scan time)
- Optimal batch calculation: 297,478 ÷ 10,000 = 30 batches
- Created 30 modulo tasks (remainder 0 to 29)
- Each task processes ~9,916 unique queries
- No batch storage - workers filter file and create batches on-the-fly

Step 3: Worker Processing
- 30 tasks submitted to Celery
- 4 workers processing in parallel
- Each worker creates its batch by filtering the file

Step 4: Processing
- Worker 1: Filters remainder=0, processes ~10,000 queries → 1.8 min
- Worker 2: Filters remainder=1, processes ~10,000 queries → 1.9 min
- ...
- Total time: ~15 minutes with 4 workers

Step 5: Results
- 297,478 unique queries processed
- 259,602 duplicate queries mapped
- Results stored in Iceberg warehouse
- Session marked as completed
```

### Monitoring Progress

```bash
# Check session status
GET /status/session_abc123

Response:
{
    "session_id": "session_abc123",
    "status": "processing",
    "progress": {
        "percentage": 67,
        "completed_batches": 20,
        "total_batches": 30,
        "failed_batches": 0
    },
    "performance": {
        "elapsed_time": "10 minutes",
        "estimated_remaining": "5 minutes",
        "queries_processed": 200000,
        "queries_per_second": 333
    }
}
```

---

## Error Handling

### Common Error Scenarios

1. **Parquet File Corruption**
   - Detection: File read error
   - Action: Skip file, log error, continue with other files

2. **Redis Connection Loss**
   - Detection: Redis timeout
   - Action: Retry with exponential backoff

3. **Worker Crash**
   - Detection: Task timeout
   - Action: Task returns to queue, picked up by another worker

4. **Memory Overflow**
   - Detection: OOM exception
   - Action: Reduce batch size, retry

5. **SQL Transpilation Error**
   - Detection: SQLGlot exception
   - Action: Mark query as failed, continue with batch

### Error Response Format

```json
{
    "error_type": "BatchProcessingError",
    "batch_id": "batch_001",
    "error_message": "Transpilation failed for 5 queries",
    "failed_queries": [
        {
            "query_hash": "hash_123",
            "error": "Unsupported syntax"
        }
    ],
    "recovery_action": "Batch marked for retry"
}
```

---

## Configuration

### System Configuration

```yaml
# config.yaml
batch_processing:
  default_batch_size: 10000
  max_batch_size: 20000
  min_batch_size: 1000
  
redis:
  host: localhost
  port: 6379
  db: 0
  batch_ttl: 3600  # 1 hour
  session_ttl: 86400  # 24 hours
  
celery:
  broker_url: redis://localhost:6379/0
  result_backend: redis://localhost:6379/0
  task_time_limit: 300  # 5 minutes
  task_soft_time_limit: 240  # 4 minutes
  worker_prefetch_multiplier: 1
  worker_concurrency: 4
  
transpilation:
  from_dialect: databricks
  to_dialect: e6
  timeout_per_query: 1  # second
  
storage:
  type: iceberg
  warehouse_path: ./iceberg_warehouse
  table_name: batch_statistics
```

---

## Future Enhancements

1. **Adaptive Batch Sizing**: Automatically adjust batch size based on query complexity
2. **Priority Queue**: High-priority batches processed first
3. **Incremental Processing**: Skip already processed queries
4. **Multi-Dialect Support**: Process to multiple target dialects in parallel
5. **Real-time Streaming**: Process queries as they arrive via Kafka/Kinesis
6. **ML-based Optimization**: Predict processing time and optimize batch distribution
7. **Query Caching**: Cache frequently used transpilation results
8. **Cross-Session Caching**: Share processed results across multiple sessions

---

## Conclusion

This architecture provides a robust, scalable solution for processing large volumes of SQL queries with:
- **Efficiency** through batch dictionary processing
- **Reliability** through atomic batch processing and recovery
- **Scalability** through distributed worker architecture
- **Simplicity** through single API entry point

The system can process 500k+ queries in approximately 15-20 minutes with 4-8 workers, processing only the unique queries present in the batch dictionaries.
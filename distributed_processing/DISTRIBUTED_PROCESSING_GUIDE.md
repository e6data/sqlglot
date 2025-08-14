# Distributed Processing System - Complete Guide

## Table of Contents
1. [System Overview](#system-overview)
2. [Redis Fundamentals](#redis-fundamentals)
3. [Celery Architecture](#celery-architecture)
4. [Implementation Details](#implementation-details)
5. [Code Walkthrough](#code-walkthrough)
6. [Worker Lifecycle](#worker-lifecycle)
7. [Configuration Deep Dive](#configuration-deep-dive)

---

## System Overview

The distributed processing system enables parallel processing of SQL transpilation jobs across large parquet file datasets. It implements a **one session per parquet file** architecture where each file gets its own unique session ID and is processed independently by workers.

### Core Architecture

```
ORCHESTRATOR                REDIS                    CELERY WORKERS
(orchestrator.py)           (Broker & State)         (celery_worker.py)
      |                          |                         |
      |--Create Sessions-------->|                         |
      |--Submit Tasks----------->|                         |
      |                          |<----Poll Queue----------|
      |                          |-----Deliver Task------->|
      |                          |                         |--Process File
      |                          |<----Update Status-------|
      |<--Query Status-----------|                         |
```

### Key Design Principles

1. **One Session Per File**: Each parquet file gets a unique session ID
2. **Independent Processing**: Workers process sessions independently
3. **Real-time Tracking**: Status updates stored in Redis
4. **Fault Tolerance**: Tasks survive worker crashes
5. **Scalability**: Add/remove workers dynamically

---

## Redis Fundamentals

### What is Redis?

Redis (Remote Dictionary Server) is an **in-memory data structure store** that acts as a database, cache, and message broker.

#### Key Characteristics:
- **In-Memory**: All data stored in RAM for ultra-fast access (microsecond response times)
- **Persistent**: Can save snapshots to disk for durability
- **Data Structures**: Supports strings, hashes, lists, sets, sorted sets
- **Atomic Operations**: All operations are atomic (thread-safe)
- **Single-Threaded**: Uses one CPU core but handles 100,000+ requests/second

### Redis Data Structures Used

```python
# 1. STRING - Simple key-value
redis.set("user:1001", "John Doe")
redis.get("user:1001")  # Returns: "John Doe"

# 2. HASH - Like a Python dict inside a dict
redis.hset("session:abc", mapping={
    'file_path': 's3://bucket/file.parquet',
    'status': 'pending',
    'worker_id': 'worker-1'
})
redis.hget("session:abc", "status")  # Returns: "pending"
redis.hgetall("session:abc")  # Returns entire dict

# 3. SET - Unordered collection of unique strings
redis.sadd("sessions:pending", "session1", "session2", "session3")
redis.smembers("sessions:pending")  # Returns: {"session1", "session2", "session3"}
redis.srem("sessions:pending", "session1")  # Removes "session1"

# 4. LIST - Ordered collection (queue-like)
redis.lpush("task_queue", "task1")  # Add to left
redis.rpush("task_queue", "task2")  # Add to right
redis.lpop("task_queue")  # Remove from left
redis.rpop("task_queue")  # Remove from right
```

### Redis in Our System

```
Redis Memory Structure:
├── session:a3f2c891 (HASH) → {file_path: "s3://file1.parquet", status: "pending"}
├── session:b4d5e902 (HASH) → {file_path: "s3://file2.parquet", status: "processing"}
├── sessions:pending (SET) → {"a3f2c891", "c6f7a813"}
├── sessions:processing (SET) → {"b4d5e902"}
└── sessions:completed (SET) → {}
```

---

## Celery Architecture

### What is Celery?

Celery is a **distributed task queue** that executes work asynchronously across multiple workers.

#### How Celery Works:

```
   PRODUCER                  BROKER                    WORKERS
   (orchestrator.py)         (Redis)                   (celery_worker.py)
        |                       |                           |
        |--Submit Task--------->|                           |
        |                       |<----Worker Polls Queue----|
        |                       |-----Send Task------------>|
        |                       |                           |--Execute Task
        |                       |<----Store Result----------|
        |<--Get Result----------|                           |
```

#### Key Concepts:

**Task**: A Python function decorated with `@celery_app.task`
```python
@celery_app.task(bind=True, max_retries=3)
def process_parquet_file(self, session_id, file_path, query_column):
    # This runs on a worker machine
    return result
```

**Broker**: Message queue (Redis) that holds pending tasks
```python
celery_app = Celery('parquet_processor', broker='redis://localhost:6379/0')
```

**Worker**: Process that pulls tasks from queue and executes them
```bash
celery -A celery_worker worker --concurrency=4  # 4 parallel workers
```

**Producer**: Code that submits tasks
```python
task = process_parquet_file.apply_async(
    args=[session_id, file_path, query_column],
    queue='parquet_processing'
)
```

---

## Implementation Details

### Current vs Modified Architecture

#### Current System (One Session for ALL Files)
```python
session_id = self.redis_manager.create_session(parquet_files, query_column)
# This creates ONE session for ALL files

for file_path in parquet_files:
    task = process_parquet_file.apply_async(args=[session_id, file_path, ...])
    self.redis_manager.add_job(session_id, task.id, file_path)
```

**Current Redis Structure:**
```
session:abc-123 = {
    total_files: 10,
    query_column: "query_string",
    status: "created"
}

session:abc-123:jobs = [task1, task2, task3, ..., task10]
```

#### Modified Architecture (One Session Per Parquet File)

```python
file_sessions = {}  # Map file -> session_id

for file_path in parquet_files:
    # CREATE INDIVIDUAL SESSION FOR EACH FILE
    session_id = self.redis_manager.create_file_session(
        file_path=file_path,
        query_column=query_column
    )
    file_sessions[file_path] = session_id
    
    # Submit ONE task per session
    task = process_parquet_file.apply_async(
        args=[session_id, file_path, query_column, from_sql, to_sql],
        queue='parquet_processing'
    )
```

**New Redis Structure:**
```
# Each file gets its own session
session:aaa-111 = {file_path: "s3://file1.parquet", status: "completed"}
session:bbb-222 = {file_path: "s3://file2.parquet", status: "processing"}
session:ccc-333 = {file_path: "s3://file3.parquet", status: "pending"}

# Each session has exactly ONE job
session:aaa-111:jobs = [task1]
session:bbb-222:jobs = [task2]
session:ccc-333:jobs = [task3]
```

---

## Code Walkthrough

### redis_manager.py - Line by Line

```python
"""
Simple Redis Manager - One session per parquet file
"""
import redis
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
```
- **redis**: Python client library to communicate with Redis server
- **json**: Convert Python dicts to strings (Redis only stores strings)
- **uuid**: Generate unique session IDs
- **datetime**: Timestamp tracking

#### Class Initialization
```python
class RedisManager:
    def __init__(self):
        self.client = redis.Redis(
            host='localhost',      # Redis server location
            port=6379,             # Default Redis port
            db=0,                  # Redis has 16 databases (0-15)
            decode_responses=True  # Auto-convert bytes to strings
        )
        self.client.ping()  # Test connection - raises error if Redis is down
```

#### Creating a Session for Each Parquet File
```python
def create_file_session(self, file_path: str, query_column: str) -> str:
    session_id = str(uuid.uuid4())[:8]  # Generate ID like "a3f2c891"
    
    session_data = {
        'session_id': session_id,
        'file_path': file_path,           # s3://bucket/file1.parquet
        'file_name': file_path.split('/')[-1],  # Just "file1.parquet"
        'query_column': query_column,      # Column containing SQL queries
        'status': 'pending',               # Initial state
        'created_at': datetime.now().isoformat(),  # 2024-01-15T10:30:00
        'worker_id': None,                 # No worker assigned yet
        'task_id': None                    # No Celery task yet
    }
    
    # Store session as Redis HASH
    self.client.hset(f"session:{session_id}", mapping=session_data)
    
    # Add to pending queue (Redis SET)
    self.client.sadd("sessions:pending", session_id)
    
    return session_id
```

**What happens in Redis:**
```
KEY: "session:a3f2c891"
VALUE: {
    session_id: "a3f2c891",
    file_path: "s3://bucket/file1.parquet",
    status: "pending",
    ...
}

KEY: "sessions:pending"
VALUE: {"a3f2c891", "b4d5e902", "c6f7a813"}  # All pending sessions
```

#### Assigning Worker to Session
```python
def assign_worker_to_session(self, session_id: str, worker_id: str, task_id: str):
    # Update existing hash fields
    self.client.hset(f"session:{session_id}", mapping={
        'worker_id': worker_id,    # e.g., "worker-1@hostname"
        'task_id': task_id,        # Celery task ID
        'status': 'processing',
        'started_at': datetime.now().isoformat()
    })
    
    # Move session between sets
    self.client.srem("sessions:pending", session_id)   # Remove from pending
    self.client.sadd("sessions:processing", session_id) # Add to processing
```

**Redis state change:**
```
Before:
  sessions:pending → {"a3f2c891", "b4d5e902"}
  sessions:processing → {}

After:
  sessions:pending → {"b4d5e902"}
  sessions:processing → {"a3f2c891"}
```

#### Updating Session Status
```python
def update_session_status(self, session_id: str, status: str, result: Optional[Dict] = None):
    updates = {
        'status': status,  # 'completed' or 'failed'
        'updated_at': datetime.now().isoformat()
    }
    
    if status == 'completed' or status == 'failed':
        updates['finished_at'] = datetime.now().isoformat()
        
        # Move to final state
        self.client.srem("sessions:processing", session_id)
        self.client.sadd(f"sessions:{status}", session_id)
    
    if result:
        updates['result'] = json.dumps(result)  # Convert dict to JSON string
    
    self.client.hset(f"session:{session_id}", mapping=updates)
```

### celery_worker.py - Detailed Explanation

#### Imports and Setup
```python
from celery import Celery, current_task
import requests
import logging
from datetime import datetime
from typing import Dict, Any
from redis_manager import RedisManager
```

- **`Celery`**: Main class to create the Celery application
- **`current_task`**: Global object to access the currently executing task's metadata
- **`requests`**: HTTP library to call the batch_statistics_s3 API

#### Logging Configuration
```python
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
```

Output format: `[2024-01-15 10:30:45] INFO - Worker starting session abc123`

#### Creating Celery Application
```python
app = Celery(
    'parquet_worker',                    # Application name
    broker='redis://localhost:6379/0',   # Where tasks are queued
    backend='redis://localhost:6379/0'   # Where results are stored
)
```

**Redis URL breakdown:**
```
redis://localhost:6379/0
  │        │       │   └── Database number (0-15)
  │        │       └────── Port number
  │        └────────────── Host
  └──────────────────────── Protocol
```

#### Celery Configuration
```python
app.conf.update(
    task_serializer='json',          # Task encoding format
    accept_content=['json'],         # Accept only JSON
    result_serializer='json',        # Result encoding format
    timezone='UTC',                  # Use UTC everywhere
    enable_utc=True,
    task_track_started=True,         # Update status when task starts
    task_time_limit=7200,            # 2 hour hard limit
    task_soft_time_limit=7000,       # Soft warning at ~1h 56min
    worker_prefetch_multiplier=1,    # CRITICAL: Take 1 task at a time
    task_acks_late=True,             # ACK after completion (crash safety)
)
```

#### Main Task Function
```python
@app.task(bind=True, name='process_parquet_session')
def process_parquet_session(
    self,
    session_id: str,
    file_path: str,
    query_column: str,
    from_sql: str = 'snowflake',
    to_sql: str = 'e6'
) -> Dict[str, Any]:
```

- `@app.task`: Registers function as Celery task
- `bind=True`: Passes task instance as `self`
- `name='process_parquet_session'`: Task name in queue

#### Worker Identification
```python
worker_id = f"{self.request.hostname}:{self.request.id[:8]}"
task_id = self.request.id
```

Example: `"worker@MacBook-Pro.local:a7c2e8f4"`

#### Processing Flow
```python
# 1. Assign worker to session
redis_manager.assign_worker_to_session(session_id, worker_id, task_id)

# 2. Update Celery task state
current_task.update_state(
    state='PROCESSING',
    meta={'session_id': session_id, 'file': file_path}
)

# 3. Call API
api_url = "http://localhost:8080/batch-statistics-s3"
payload = {
    's3_path': file_path,
    'query_column': query_column,
    'from_sql': from_sql,
    'to_sql': to_sql,
    'memory_threshold_mb': 500,
    'batch_size': 50000
}
response = requests.post(api_url, data=payload, timeout=7200)

# 4. Update session with results
redis_manager.update_session_status(session_id, 'completed', session_result)
```

---

## Worker Lifecycle

### 1. Worker Startup
```
Terminal: python celery_worker.py
    ↓
Celery spawns 4 processes
    ↓
Each process connects to Redis
    ↓
Processes start polling queue
```

### 2. Task Execution Flow
```
ORCHESTRATOR                    REDIS                         WORKER PROCESS
     |                           |                                  |
     |--Submit task------------->|                                  |
     |  session_id='abc'         |                                  |
     |  file='s3://file.parquet' |                                  |
     |                           |<-------Poll queue (BRPOP)--------|
     |                           |                                  |
     |                           |--------Deliver task------------->|
     |                           |                                  |
     |                           |                          Execute process_parquet_session()
     |                           |                                  |
     |                           |<----Update session:abc----------|
     |                           |     status='processing'         |
     |                           |                                  |
     |                           |                          Call API (may take minutes)
     |                           |                                  |
     |                           |<----Update session:abc----------|
     |                           |     status='completed'          |
     |                           |     result={...}                |
     |                           |                                  |
     |                           |<----Store Celery result---------|
```

### 3. Concurrent Processing Example

With `--concurrency=4`, you have 4 worker processes:

```
Time →
Worker 1: [Session A █████████████] [Session E ███████]
Worker 2: [Session B ████████] [Session F ██████████]  
Worker 3: [Session C ███████████████████]
Worker 4: [Session D ██████] [Session G ████] [Session H ███]

Queue:    [I] [J] [K] [L] [M] ...
```

Each worker:
- Processes ONE session at a time (worker_prefetch_multiplier=1)
- Pulls next task only after completing current
- Updates Redis session status independently

### 4. Error Handling and Retries

```python
# If API call fails:
Worker → Update session:failed → Raise exception
                                        ↓
                              Celery retry logic:
                              - Retry 3 times (max_retries=3)
                              - Exponential backoff
                              - Return task to queue
```

### 5. Worker Shutdown

```
Ctrl+C in terminal
    ↓
SIGINT signal to Celery
    ↓
Celery graceful shutdown:
1. Stop accepting new tasks
2. Wait for current tasks to complete
3. Save task state to Redis
4. Disconnect from Redis
5. Exit
```

Force shutdown (Ctrl+C twice):
- Immediately terminates
- Current tasks marked as failed
- Tasks return to queue (if acks_late=True)

---

## Configuration Deep Dive

### Critical Settings Explained

#### worker_prefetch_multiplier=1 vs concurrency=4

These settings are **NOT contradictory** - they work together perfectly:

- `worker_prefetch_multiplier=1`: Controls **how many tasks a worker RESERVES from queue**
- `--concurrency=4`: Controls **how many WORKER PROCESSES exist**

#### Visual Comparison

**WITH prefetch=1 and concurrency=4 (Our Setup):**
```
REDIS QUEUE                     4 WORKER PROCESSES
[Task5][Task6][Task7]...        
                                Worker Process 1: Processing Task1
                                Worker Process 2: Processing Task2  
                                Worker Process 3: Processing Task3
                                Worker Process 4: Processing Task4

Each worker took EXACTLY 1 task
Tasks 5,6,7 remain in queue for whoever finishes first
```

**If we had prefetch=4 and concurrency=4 (BAD):**
```
REDIS QUEUE                     4 WORKER PROCESSES
[Task17][Task18]...             
                                Worker Process 1: Processing Task1
                                                  Reserved: [Task5,Task6,Task7,Task8]
                                
                                Worker Process 2: Processing Task2
                                                  Reserved: [Task9,Task10,Task11,Task12]
                                
                                Worker Process 3: Processing Task3
                                                  Reserved: [Task13,Task14,Task15,Task16]
                                
                                Worker Process 4: Processing Task4
                                                  Reserved: []  ← No tasks left!

Worker 1 hoarded tasks 5-8 even though it's still processing Task1!
```

#### Real-World Scenario

With our settings (prefetch=1, concurrency=4):
```
Time: 0 min
Worker 1: Starting file1.parquet (takes 5 min)
Worker 2: Starting file2.parquet (takes 2 min)
Worker 3: Starting file3.parquet (takes 8 min)
Worker 4: Starting file4.parquet (takes 3 min)
Queue: [file5, file6, file7, file8]

Time: 2 min
Worker 2: DONE! Grabs file5.parquet immediately
Worker 1: Still processing file1...
Worker 3: Still processing file3...
Worker 4: Still processing file4...
Queue: [file6, file7, file8]

Time: 3 min
Worker 4: DONE! Grabs file6.parquet immediately
Queue: [file7, file8]
```

If we had prefetch=4 (BAD):
```
Time: 0 min
Worker 1: Starting file1.parquet (takes 5 min)
          Reserved: [file5, file6, file7, file8] ← HOARDING!
Worker 2: Starting file2.parquet (takes 2 min)
          Reserved: [file9, file10, file11, file12]

Time: 3 min
Worker 4: DONE! But has nothing to process!
          Sits IDLE while Worker 1 hoards tasks!
```

### task_acks_late=True Explained

**Task acknowledgment timing:**

DEFAULT (acks_late=False):
```
Worker gets task → ACK to Redis → Process → Done
                   ↑
            (Task removed from queue immediately)
            (If worker crashes, task is lost!)
```

WITH acks_late=True:
```
Worker gets task → Process → Done → ACK to Redis
                                     ↑
                          (Task stays in queue until finished)
                          (If worker crashes, task returns to queue!)
```

### Combined Effect

These settings create an **optimal distributed system**:

| Setting | Purpose | Effect |
|---------|---------|--------|
| `concurrency=4` | 4 parallel workers | Process 4 files simultaneously |
| `prefetch=1` | Each worker takes 1 task | No hoarding, fair distribution |
| `acks_late=True` | ACK after completion | Crash recovery |

**Together they ensure:**
- Maximum parallelism (4 workers)
- Fair task distribution (prefetch=1)
- Fault tolerance (acks_late=True)
- No idle workers while tasks exist

---

## Complete Workflow Example

### Processing 3 Parquet Files

**Initial State:**
```python
# Create sessions for 3 files
manager = RedisManager()
session1 = manager.create_file_session("s3://bucket/file1.parquet", "query_column")
session2 = manager.create_file_session("s3://bucket/file2.parquet", "query_column")
session3 = manager.create_file_session("s3://bucket/file3.parquet", "query_column")
```

**Redis State After Creation:**
```
REDIS MEMORY:
============================
HASHES (Individual Sessions):
  session:a3f2c891 → {
    file_path: "s3://bucket/file1.parquet",
    status: "pending",
    worker_id: None
  }
  session:b4d5e902 → {
    file_path: "s3://bucket/file2.parquet",
    status: "pending",
    worker_id: None
  }
  session:c6f7a813 → {
    file_path: "s3://bucket/file3.parquet",
    status: "pending",
    worker_id: None
  }

SETS (Queue Management):
  sessions:pending → {"a3f2c891", "b4d5e902", "c6f7a813"}
  sessions:processing → {}
  sessions:completed → {}
  sessions:failed → {}
```

**Workers Pick Up Sessions:**
```python
manager.assign_worker_to_session("a3f2c891", "worker-1", "celery-task-123")
```

**Timeline:**
```
TIME →

T=0s   Create 3 sessions
       Redis: pending={s1, s2, s3}, processing={}, completed={}
       
T=1s   Worker1 picks s1, Worker2 picks s2
       Redis: pending={s3}, processing={s1, s2}, completed={}
       
T=60s  Worker1 completes s1, Worker3 picks s3
       Redis: pending={}, processing={s2, s3}, completed={s1}
       
T=120s Worker2 completes s2
       Redis: pending={}, processing={s3}, completed={s1, s2}
       
T=150s Worker3 fails s3
       Redis: pending={}, processing={}, completed={s1, s2}, failed={s3}
```

### Real-World Execution

```bash
# Terminal 1: Start Redis
redis-server

# Terminal 2: Start Workers
python celery_worker.py
# Output:
[2024-01-15 10:00:00] INFO - Connected to redis://localhost:6379/0
[2024-01-15 10:00:00] INFO - celery@worker ready
[2024-01-15 10:00:00] INFO - Started 4 worker processes

# Terminal 3: Submit tasks (from orchestrator)
# Task submitted for s3://bucket/file1.parquet

# Back in Terminal 2:
[2024-01-15 10:00:05] INFO - Worker worker@host:a7c2e8f4 starting session abc123
[2024-01-15 10:00:05] INFO - Processing file: s3://bucket/file1.parquet
[2024-01-15 10:00:05] INFO - Calling API for s3://bucket/file1.parquet
[2024-01-15 10:02:35] INFO - Session abc123 completed successfully. Processed 10000 queries in 150.00s
```

---

## Benefits of This Architecture

1. **Independent Tracking**: Each file's progress tracked separately
2. **Granular Control**: Can retry/cancel individual files
3. **Parallel Processing**: Workers truly independent, no shared state
4. **Better Monitoring**: See exactly which files completed/failed
5. **Easier Debugging**: Issues isolated to specific session IDs
6. **Scalability**: Can process 1000s of files without session bloat
7. **Reliability**: Tasks survive crashes
8. **Visibility**: Real-time status in Redis
9. **Efficiency**: Parallel processing of multiple files

## Summary

This distributed processing system leverages Redis for state management and Celery for task distribution to achieve:

- **10-100x faster processing** through parallelization
- **Reliability** through retries and persistence  
- **Visibility** through real-time progress tracking
- **Scalability** from 1 to 1000 workers
- **Simplicity** compared to building custom solutions

For processing hundreds of large parquet files with SQL transpilation, this architecture provides the optimal balance of performance, reliability, and maintainability.
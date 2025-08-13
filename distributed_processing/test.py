#!/usr/bin/env python3
"""
Test distributed processing for entire directory
"""
import requests
import time
import json
from redis_manager import RedisJobManager
from celery_worker import process_parquet_file

# Configuration
S3_PATH = "s3://customers-sampledata/CPPI/queries/"
API_URL = "http://localhost:8080"

print("Starting distributed processing test...")
print("=" * 60)

# 1. Validate S3 bucket and get all files
print("\n1. Validating S3 bucket and getting all parquet files...")
response = requests.post(f"{API_URL}/validate-s3-bucket", data={'s3_path': S3_PATH})
validation = response.json()

print(f"   Files found: {validation.get('files_found', 0)}")
print(f"   Query column: {validation.get('query_column', 'Not detected')}")
print(f"   Total size: {validation.get('total_size_mb', 0)} MB")

# Use hardcoded column name as requested
query_column = 'query_string'

# Get ALL parquet files
files = validation.get('sample_files', [])
if not files:
    print("No files found!")
    exit(1)

# Convert to full S3 paths
all_files = []
for file in files:
    if file.startswith('customers-sampledata/'):
        all_files.append(f"s3://{file}")
    else:
        all_files.append(file)

print(f"\n2. Processing ALL {len(all_files)} parquet files:")
for i, file in enumerate(all_files[:5], 1):  # Show first 5
    print(f"   {i}. {file.split('/')[-1]}")
if len(all_files) > 5:
    print(f"   ... and {len(all_files) - 5} more files")

# Create session
manager = RedisJobManager()
session_id = manager.create_session(all_files, query_column)
print(f"\n3. Session created: {session_id}")

# Submit ALL jobs
print(f"\n4. Submitting {len(all_files)} jobs to Celery workers...")
task_ids = []
for i, file_path in enumerate(all_files, 1):
    task = process_parquet_file.apply_async(
        args=[session_id, file_path, query_column, "athena", "e6", None],
        queue='parquet_processing'
    )
    manager.add_job(session_id, task.id, file_path)
    task_ids.append(task.id)
    
    if i % 10 == 0:  # Progress update
        print(f"   Submitted {i}/{len(all_files)} jobs...")

print(f"   All {len(task_ids)} jobs submitted!")

# Monitor progress
print(f"\n5. Monitoring progress (this may take several minutes)...")
print("   Progress will be shown every 10 seconds...")

start_time = time.time()
last_update = 0

while True:
    status = manager.get_session_status(session_id)
    summary = status['summary']
    
    # Show progress every 10 seconds
    current_time = time.time()
    if current_time - last_update >= 10 or summary['completed'] + summary['failed'] == summary['total']:
        elapsed = int(current_time - start_time)
        print(f"   [{elapsed:3d}s] Progress: {summary['progress']:5.1f}% | "
              f"Completed: {summary['completed']:2d} | "
              f"Failed: {summary['failed']:2d} | "
              f"Processing: {summary['processing']:2d} | "
              f"Pending: {summary['pending']:2d}")
        last_update = current_time
    
    # Check if all done
    if summary['completed'] + summary['failed'] == summary['total']:
        print(f"\n6. ALL JOBS COMPLETED!")
        print(f"   Total time: {int(time.time() - start_time)} seconds")
        break
    
    time.sleep(2)

# Final results
print("\n7. FINAL RESULTS:")
final_status = manager.get_session_status(session_id)
summary = final_status['summary']

print(f"   Total files processed: {summary['total']}")
print(f"   Successfully completed: {summary['completed']}")
print(f"   Failed: {summary['failed']}")
print(f"   Success rate: {summary['progress']:.1f}%")

# Show some successful job details
successful_jobs = [job for job in final_status.get('jobs', []) if job.get('status') == 'completed']
if successful_jobs:
    print(f"\n8. SAMPLE SUCCESSFUL RESULTS (first 3):")
    for i, job in enumerate(successful_jobs[:3], 1):
        result = job.get('result', {})
        file_name = job.get('file_path', '').split('/')[-1]
        print(f"   {i}. {file_name}")
        print(f"      Queries processed: {result.get('total_queries', 0)}")
        print(f"      Success rate: {result.get('success_rate', 'N/A')}")
        print(f"      Processing time: {result.get('processing_time', 0):.2f}s")

print("\n" + "=" * 60)
print("WHERE TO GET RESULTS:")
print("=" * 60)
print(f"SESSION ID: {session_id}")
print(f"To get full results later, run:")
print(f"  python -c \"from redis_manager import RedisJobManager; import json;")
print(f"  m = RedisJobManager(); print(json.dumps(m.get_session_status('{session_id}'), indent=2))\"")
print("=" * 60)
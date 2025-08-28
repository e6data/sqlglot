"""
Simplified Orchestrator using Celery's built-in features
No explicit Redis management needed - Celery handles it all
"""
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
import uuid
import hashlib
import re
import numpy as np
from datetime import datetime
import dateutil.parser
from celery import group, chord, signature
from .worker import celery as celery_app
from .tasks import discover_parquet_files
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import s3fs

logger = logging.getLogger(__name__)


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable format
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = int(seconds % 60)
        return f"{minutes}m {remaining_seconds}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        remaining_seconds = int(seconds % 60)
        return f"{hours}h {minutes}m {remaining_seconds}s"


def extract_unique_queries_from_file(
    file_path: str, 
    query_column: str, 
    filters: Dict[str, Any]
) -> pa.Table:
    """
    Read file once and extract unique queries with filtering - returns PyArrow Table
    This replaces the repetitive file reading in workers and avoids Python list conversions
    """
    try:
        if file_path.startswith('s3://'):
            # S3 file reading using s3fs
            credentials = {
                "access_key":"ASIAZYHN7XI64V6RB3JE",
                "secret_key":"ivFKpPAYVeLxKVAHzwBm5UvUw95jI2eOuXoWop5t",
                "session_token":"FwoGZXIvYXdzEFYaDJYO/Msc2RGRhHkyNCLWAVEJ/q5S2bfCV6fYnnOO8AbEP0PdPyEKpE5xxFiJ2CC8ocmffBUUf59VUk0JQiEbljmqsyg7aOUkwm4zHUk4NYidd/2fSakcuawYV0QnL6ZbKMOjPN1wlCaXJYsDPXCvcuGXKP5FWXvJsmLcrLG0YQeLzC3DWfxjacAPinZAKOKrA/YkzXwVslYqM+hDK+fjqwiVK3BHFFXn4kUkI3uBrtJW94hueIG5dvSMYL4C7A/7I9wHLIC+zVEYCd3Tch95X1x8K+VBt4ayFdtiaAHY0oJ6K+zhTWEok8K/xQYyM84wjGOZFVNzChrNGcUhY1ph1KmVh5kYc58relyWJ992BU0WdNNW4T9VuFttIbwxnbv6Kw==",
                "region": "us-east-1"
            }
            
            s3fs_fs = s3fs.S3FileSystem(
                key=credentials["access_key"],
                secret=credentials["secret_key"], 
                token=credentials["session_token"],
                client_kwargs={'region_name': credentials["region"]},
                config_kwargs={'connect_timeout': 30, 'read_timeout': 60}
            )
            
            logger.info(f"📖 Reading S3 file: {file_path}")
            table = pq.read_table(file_path, filesystem=s3fs_fs)
        else:
            # Local file reading
            logger.info(f"📖 Reading local file: {file_path}")
            table = pq.read_table(file_path)
        
        logger.info(f"Loaded {len(table):,} rows from file")
        
        # Apply filters using PyArrow compute functions (vectorized)
        filter_conditions = []
        filter_conditions.append(pc.is_valid(table[query_column]))
        filter_conditions.append(pc.not_equal(table[query_column], ""))
        
        for filter_col, filter_value in filters.items():
            if filter_col in table.schema.names:
                try:
                    # Support both single values and multiple values (lists)
                    if isinstance(filter_value, list):
                        if len(filter_value) > 0:  # Only apply filter if list is not empty
                            # Use PyArrow's is_in for multiple values (IN operator)
                            logger.info(f"Applying multi-value filter: {filter_col} IN {filter_value}")
                            filter_conditions.append(pc.is_in(table[filter_col], pa.array(filter_value)))
                        else:
                            logger.warning(f"Empty list provided for filter column '{filter_col}', skipping filter")
                    else:
                        # Single value - use exact match (backward compatible)
                        logger.info(f"Applying single-value filter: {filter_col} = {filter_value}")
                        filter_conditions.append(pc.equal(table[filter_col], filter_value))
                except Exception as e:
                    logger.error(f"Error applying filter for column '{filter_col}' with value {filter_value}: {str(e)}")
                    logger.error(f"Column '{filter_col}' data type: {table[filter_col].type}")
                    # Skip this filter and continue with others
                    continue
            else:
                logger.warning(f"Filter column '{filter_col}' not found in parquet file. Available columns: {table.schema.names}")
        
        if filter_conditions:
            combined_filter = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_filter = pc.and_kleene(combined_filter, condition)
            
            table = table.filter(combined_filter)
            logger.info(f"After filtering: {len(table):,} rows")
        
        # Deduplicate queries using PyArrow group_by (vectorized)
        if len(table) == 0:
            return pa.table({query_column: pa.array([], type=pa.string())})
        
        unique_table = table.select([query_column]).group_by([query_column]).aggregate([])
        
        logger.info(f"Extracted {len(unique_table):,} unique queries")
        return unique_table
        
    except Exception as e:
        logger.error(f"Failed to extract queries from {file_path}: {str(e)}")
        return pa.table({query_column: pa.array([], type=pa.string())})


def create_query_batch_configs(
    unique_table: pa.Table,
    session_id: str,
    company_name: str,
    from_dialect: str,
    to_dialect: str,
    query_column: str,
    batch_size: int,
    file_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Create batch configurations with PyArrow table (optimized for memory and speed)
    Uses hash-based distribution for consistent batching with vectorized operations
    """
    if len(unique_table) == 0:
        return []
    
    # Calculate number of batches needed
    num_queries = len(unique_table)
    num_batches = max(1, (num_queries + batch_size - 1) // batch_size)
    
    # SHA-256 hash-based distribution using Python hashlib (universally available)
    # Convert queries to Python list for hashing
    query_list = unique_table[query_column].to_pylist()
    
    # Use SHA-256 for consistent hash distribution
    hash_values = []
    for query in query_list:
        # Create SHA-256 hash of query string
        hash_obj = hashlib.sha256(query.encode('utf-8'))
        # Convert first 8 bytes to integer for batch assignment
        hash_int = int.from_bytes(hash_obj.digest()[:8], 'big')
        batch_id = hash_int % num_batches
        hash_values.append(batch_id)
    
    # Convert back to PyArrow array
    batch_ids = pa.array(hash_values, type=pa.int32())
    
    # Add batch_id column to table
    table_with_batch = unique_table.append_column('batch_id', batch_ids)
    
    # Create configurations using PyArrow operations (eliminate Python lists)
    # Get unique batch IDs that actually have data
    unique_batch_ids = pc.unique(table_with_batch['batch_id'])
    num_actual_batches = len(unique_batch_ids)
    
    # Create batch config table using PyArrow operations
    batch_config_data = {
        'batch_id': [],
        'session_id': [],
        'file_name': [],
        'batch_idx': [],
        'total_batches': [],
        'company_name': [],
        'from_dialect': [],
        'to_dialect': [],
        'query_column': [],
        'queries_table': [],
        'query_count': []
    }
    
    # Process each batch using vectorized operations
    for batch_idx_scalar in unique_batch_ids.to_pylist():  # Only convert scalars
        batch_idx = int(batch_idx_scalar)
        
        # Filter table for this batch (vectorized)
        batch_mask = pc.equal(table_with_batch['batch_id'], batch_idx)
        batch_table = table_with_batch.filter(batch_mask)
        
        if len(batch_table) == 0:  # Skip empty batches
            continue
        
        # Remove the batch_id column before storing
        batch_queries_table = batch_table.select([query_column])
        
        # Convert PyArrow table to serializable format for Celery
        queries_list = batch_queries_table[query_column].to_pylist()
        
        # Collect batch config data (convert to serializable format)
        batch_config_data['batch_id'].append(f"batch_{session_id}_{batch_idx}")
        batch_config_data['session_id'].append(session_id)
        batch_config_data['file_name'].append(file_config['file_name'])
        batch_config_data['batch_idx'].append(batch_idx)
        batch_config_data['total_batches'].append(num_batches)
        batch_config_data['company_name'].append(company_name)
        batch_config_data['from_dialect'].append(from_dialect)
        batch_config_data['to_dialect'].append(to_dialect)
        batch_config_data['query_column'].append(query_column)
        batch_config_data['queries_table'].append(queries_list)  # Convert to Python list for serialization
        batch_config_data['query_count'].append(len(queries_list))
    
    # Convert to list of dicts (needed for Celery serialization)
    batch_configs = []
    for i in range(len(batch_config_data['batch_id'])):
        batch_config = {
            'batch_id': batch_config_data['batch_id'][i],
            'session_id': batch_config_data['session_id'][i],
            'file_name': batch_config_data['file_name'][i],
            'batch_idx': batch_config_data['batch_idx'][i],
            'total_batches': batch_config_data['total_batches'][i],
            'company_name': batch_config_data['company_name'][i],
            'from_dialect': batch_config_data['from_dialect'][i],
            'to_dialect': batch_config_data['to_dialect'][i],
            'query_column': batch_config_data['query_column'][i],
            'queries_table': batch_config_data['queries_table'][i],  # Python list now
            'query_count': batch_config_data['query_count'][i]
        }
        batch_configs.append(batch_config)
    
    logger.info(f"Created {len(batch_configs)} batches from {num_queries} queries using vectorized operations")
    return batch_configs


def orchestrate_processing(
    directory_path: str,
    company_name: str,
    from_dialect: str,
    to_dialect: str,
    query_column: str,
    batch_size: int = 10000,
    filters: Dict[str, Any] = None,
    name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Orchestrate the entire processing pipeline using Celery with optimized file reading
    NEW APPROACH: Read each file once in orchestrator, distribute queries to workers
    
    Returns immediately with task group ID that can be monitored
    Following TestDriven.io pattern - let Celery handle all the complexity
    """
    if name and name.strip():
        # Use custom name with fallback to short UUID
        clean_name = re.sub(r'[^a-zA-Z0-9_-]', '_', name.strip())[:20]  # Sanitize and limit length
        clean_name = clean_name.strip('_')  # Remove leading/trailing underscores
        # if clean_name and not clean_name[0].isalnum():  # Ensure it starts with alphanumeric
        #     clean_name = 'n_' + clean_name
        # if not clean_name:  # Fallback if name becomes empty after cleaning
        #     clean_name = 'custom'
        session_id = f"session_{clean_name}_{uuid.uuid4().hex[:8]}"
    else:
        # Default behavior
        session_id = f"session_{uuid.uuid4().hex[:8]}"
    
    logger.info(f"🚀 Starting orchestration for session {session_id}")
    logger.info(f"📂 Processing path: {directory_path}")
    
    # Check if it's an S3 path
    is_s3_path = directory_path.startswith('s3://')
    if is_s3_path:
        logger.info("📦 Using S3 filesystem with temporary credentials")
    
    try:
        # Discover and validate files (now supports S3)
        file_configs = discover_parquet_files(
            directory_path,
            query_column
        )
        
        if not file_configs:
            return {
                'error': f'No valid parquet files found in {directory_path}',
                'session_id': session_id
            }
        
        # NEW APPROACH: Read files and extract queries in orchestrator using PyArrow
        all_batch_configs = []
        
        # Process all files and collect batch configs
        for file_config in file_configs:
            logger.info(f"📖 Reading and processing file: {file_config['file_name']}")
            
            # Extract unique queries from this file as PyArrow table
            unique_table = extract_unique_queries_from_file(
                file_config['file_path'],
                query_column,
                filters or {}
            )
            
            if len(unique_table) == 0:
                logger.warning(f"No queries found in {file_config['file_name']}")
                continue
            
            logger.info(f"✅ Extracted {len(unique_table):,} unique queries from {file_config['file_name']}")
            
            # Create batch configurations with PyArrow table
            file_batch_configs = create_query_batch_configs(
                unique_table,
                session_id,
                company_name,
                from_dialect,
                to_dialect,
                query_column,
                batch_size,
                file_config
            )
            
            # Extend configs (still need this for Celery task creation)
            all_batch_configs.extend(file_batch_configs)
        
        if not all_batch_configs:
            return {
                'error': 'No valid queries found in any files',
                'session_id': session_id
            }
        
        logger.info(f"📦 Created {len(all_batch_configs)} batch configurations with pre-loaded queries")
        
        # Create a Celery group for parallel processing
        # Each batch now contains actual queries, not file paths
        job = group(
            celery_app.signature(
                'process_query_batch',  # New task for processing query lists
                args=[config],
                task_id=f"{session_id}_{config['batch_id']}"  # Custom task ID for tracking
            )
            for config in all_batch_configs
        )
        
        # Apply async and get the group result
        group_result = job.apply_async()
        
        # Store session metadata in Redis
        start_time = datetime.now().isoformat()
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0)
            session_meta_key = f"session_meta_{session_id}"
            r.hset(session_meta_key, 'start_time', start_time)
            r.hset(session_meta_key, 'total_batches', len(all_batch_configs))
            r.expire(session_meta_key, 86400)  # Expire after 24 hours
        except Exception as e:
            logger.warning(f"Failed to store session metadata: {e}")
        
        logger.info(f"✅ Launched {len(all_batch_configs)} tasks for processing")
        
        # Return immediately with tracking information
        return {
            'session_id': session_id,
            'group_id': group_result.id if hasattr(group_result, 'id') else session_id,
            'status': 'processing',
            'total_files': len(file_configs),
            'total_batches': len(all_batch_configs),
            'task_ids': [f"{session_id}_{config['batch_id']}" for config in all_batch_configs],
            'created_at': start_time,
            'start_time': start_time,  # Include start time immediately
            'configuration': {
                'directory_path': directory_path,
                'company_name': company_name,
                'from_dialect': from_dialect,
                'to_dialect': to_dialect,
                'batch_size': batch_size
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Orchestration failed: {str(e)}")
        return {
            'error': str(e),
            'session_id': session_id,
            'status': 'failed'
        }


def get_processing_status(session_id: str, task_ids: List[str] = None) -> Dict[str, Any]:
    """
    Get status of processing session using Celery's AsyncResult
    If no task_ids provided, search Redis for tasks matching session pattern
    Special case: if session_id is 'discover_all', return all unique session IDs
    """
    from celery.result import AsyncResult
    import redis
    
    # Special case: discover all active sessions
    if session_id == 'discover_all':
        try:
            r = redis.Redis(host='localhost', port=6379, db=0)
            keys = r.keys('celery-task-meta-*')
            discovered_sessions = set()
            
            for key in keys:
                task_id = key.decode().replace('celery-task-meta-', '')
                # Extract session ID from task ID (format: session_xxx_batch_xxx)
                if task_id.startswith('session_'):
                    # Split by '_batch_' to get session part
                    parts = task_id.split('_batch_')
                    if len(parts) >= 2:
                        session_part = parts[0]  # Everything before '_batch_'
                        discovered_sessions.add(session_part)
            
            # Also check for session metadata keys
            session_meta_keys = r.keys('session_meta_*')
            for key in session_meta_keys:
                session_meta_id = key.decode().replace('session_meta_', '')
                discovered_sessions.add(session_meta_id)
            
            logger.info(f"Discovered {len(discovered_sessions)} unique sessions in Redis")
            return {
                'session_id': 'discover_all',
                'discovered_sessions': list(discovered_sessions),
                'total_discovered': len(discovered_sessions)
            }
        except Exception as e:
            logger.error(f"Failed to discover sessions from Redis: {e}")
            return {
                'session_id': 'discover_all',
                'discovered_sessions': [],
                'total_discovered': 0,
                'error': str(e)
            }
    
    if not task_ids:
        # Search Redis for completed tasks matching session pattern
        try:
            r = redis.Redis(host='localhost', port=6379, db=0)
            keys = r.keys('celery-task-meta-*')
            task_ids = []
            
            for key in keys:
                task_id = key.decode().replace('celery-task-meta-', '')
                if task_id.startswith(session_id):
                    task_ids.append(task_id)
                    
            logger.info(f"Found {len(task_ids)} tasks in Redis for session {session_id}")
        except Exception as e:
            logger.error(f"Failed to search Redis for tasks: {e}")
            task_ids = []
    
    results = []
    completed = 0
    failed = 0
    pending = 0
    processing = 0
    
    # Track timing information
    start_times = []
    end_times = []
    
    for task_id in task_ids:
        result = AsyncResult(task_id)
        
        task_status = {
            'task_id': task_id,
            'state': result.state
        }
        
        if result.state == 'PENDING':
            pending += 1
            task_status['status'] = 'Waiting to be processed'
        elif result.state == 'STARTED':
            processing += 1
            task_status['status'] = 'Processing'
        elif result.state == 'PROGRESS':
            processing += 1
            if result.info:
                task_status.update(result.info)
        elif result.state == 'SUCCESS':
            completed += 1
            task_status['status'] = 'Completed'
            if result.result:
                # Only include essential result info to avoid huge responses
                if isinstance(result.result, dict):
                    task_status['processed_count'] = result.result.get('processed_count', 0)
                    task_status['successful_count'] = result.result.get('successful_count', 0)
                    # Track completion time
                    if 'completion_time' in result.result:
                        end_times.append(result.result['completion_time'])
        elif result.state == 'FAILURE':
            failed += 1
            task_status['status'] = 'Failed'
            task_status['error'] = str(result.info)
        
        results.append(task_status)
    
    total_tasks = len(task_ids)
    total_batches = total_tasks  # Each task represents a batch
    successful_batches = completed  # Only count successfully completed batches
    
    # Calculate percentage based on successful batches vs total batches
    progress_percentage = (successful_batches / total_batches * 100) if total_batches > 0 else 0
    
    # Calculate timing information and get actual total batches
    session_start_time = None
    session_end_time = None
    total_duration = None
    actual_total_batches = total_batches  # Default to calculated value
    
    # Try to get timing info and total batches from Redis metadata
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        session_meta_key = f"session_meta_{session_id}"
        session_meta = r.hgetall(session_meta_key)
        
        if session_meta:
            if b'start_time' in session_meta:
                session_start_time = session_meta[b'start_time'].decode()
            if b'end_time' in session_meta:
                session_end_time = session_meta[b'end_time'].decode()
            if b'total_batches' in session_meta:
                actual_total_batches = int(session_meta[b'total_batches'].decode())
        
        # If session is completed and no end time recorded, set it now
        if (completed + failed >= actual_total_batches) and session_start_time and not session_end_time:
            session_end_time = datetime.now().isoformat()
            r.hset(session_meta_key, 'end_time', session_end_time)
            r.expire(session_meta_key, 86400)  # Expire after 24 hours
        
        # Calculate duration if we have both times
        if session_start_time and session_end_time:
            try:
                start_dt = dateutil.parser.parse(session_start_time)
                end_dt = dateutil.parser.parse(session_end_time)
                duration_seconds = (end_dt - start_dt).total_seconds()
                total_duration = format_duration(duration_seconds)
            except Exception as e:
                logger.warning(f"Failed to calculate duration: {e}")
                
    except Exception as e:
        logger.warning(f"Failed to get session timing info: {e}")
    
    # Recalculate progress percentage with actual total batches
    progress_percentage = (successful_batches / actual_total_batches * 100) if actual_total_batches > 0 else 0
    
    return {
        'session_id': session_id,
        'total_tasks': total_tasks,
        'total_batches': actual_total_batches,  # Use actual total batches from Redis
        'completed': completed,
        'failed': failed,
        'pending': pending,
        'processing': processing,
        'successful_batches': successful_batches,  # Add this field
        'progress_percentage': round(progress_percentage, 1),
        'task_details': results,  # Return all task details
        'overall_status': 'completed' if completed + failed >= actual_total_batches else 'processing',
        'start_time': session_start_time,
        'end_time': session_end_time,
        'duration': total_duration
    }


def get_task_result(task_id: str) -> Dict[str, Any]:
    """
    Get result of a specific task using Celery's AsyncResult
    """
    from celery.result import AsyncResult
    
    result = AsyncResult(task_id)
    
    response = {
        'task_id': task_id,
        'state': result.state,
        'ready': result.ready()
    }
    
    if result.state == 'PENDING':
        response['status'] = 'Task is waiting to be processed'
    elif result.state == 'STARTED':
        response['status'] = 'Task has started processing'
    elif result.state == 'PROGRESS':
        response['status'] = 'Task is in progress'
        if result.info:
            response['progress'] = result.info
    elif result.state == 'SUCCESS':
        response['status'] = 'Task completed successfully'
        response['result'] = result.result
    elif result.state == 'FAILURE':
        response['status'] = 'Task failed'
        response['error'] = str(result.info)
        response['traceback'] = result.traceback
    elif result.state == 'RETRY':
        response['status'] = 'Task is being retried'
    else:
        response['status'] = f'Unknown state: {result.state}'
    
    return response
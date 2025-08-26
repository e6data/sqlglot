"""
Simplified Orchestrator using Celery's built-in features
No explicit Redis management needed - Celery handles it all
"""
import logging
from typing import Dict, Any, List
from pathlib import Path
import uuid
import hashlib
import numpy as np
from datetime import datetime
from celery import group, chord, signature
from .worker import celery as celery_app
from .tasks import discover_parquet_files, create_batch_configs, get_filesystem
import pyarrow.parquet as pq
import pyarrow.compute as pc
import s3fs

logger = logging.getLogger(__name__)


def extract_unique_queries_from_file(
    file_path: str, 
    query_column: str, 
    filters: Dict[str, Any]
) -> List[str]:
    """
    Read file once and extract unique queries with filtering
    This replaces the repetitive file reading in workers
    """
    try:
        if file_path.startswith('s3://'):
            # S3 file reading using s3fs
            credentials = {
                "access_key": "ASIAZYHN7XI6UIUBDZKV",
                "secret_key": "Je9sw4Vg+i4WBzRQQD6lYPnwo6o/jxIOorSQhosu", 
                "session_token": "FwoGZXIvYXdzEBAaDHaGtaOADz6xDXuDbSLWAQ5qTNEw+lcm9CfpOE3oGaFOE0Gsmc0qzD4NFZIcVzt3e3B8MScMq1mhP6mp3//xDJSqeK+3oPDwdsc2iAik0xga9yloWwjU1Wvzvi+GWNrNCOcFLbkdPZ+dvlC8KrFlyE61ZozQrFDK/1V8+ipYHfNXPOX2MYeJKEZ0+pKe7Ij6hh7qlG2rYl3uX5+6WKjnjw9ez1+VQ7IFTnpgV87EEDm/5SmEjJo9AhXf0aUgBDdbsV5UvUuVcncnfwHAK1TK+P8Mu5wio4GVmCYtRl8HJ5eP236GqRAojPuvxQYyM3o8RsOb0KjLEX9kjogwpiZ4D6pKZbbsTr02nzpZML4LUzXRUEdqra3nCYmKeoTvh5vCpg==",
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
        
        # Apply filters
        filter_conditions = []
        filter_conditions.append(pc.is_valid(table[query_column]))
        filter_conditions.append(pc.not_equal(table[query_column], ""))
        
        for filter_col, filter_value in filters.items():
            if filter_col in table.schema.names:
                filter_conditions.append(pc.equal(table[filter_col], filter_value))
        
        if filter_conditions:
            combined_filter = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_filter = pc.and_kleene(combined_filter, condition)
            
            table = table.filter(combined_filter)
            logger.info(f"After filtering: {len(table):,} rows")
        
        # Deduplicate queries
        if len(table) == 0:
            return []
        
        unique_table = table.select([query_column]).group_by([query_column]).aggregate([])
        unique_queries = unique_table[query_column].to_pylist()
        
        logger.info(f"Extracted {len(unique_queries):,} unique queries")
        return unique_queries
        
    except Exception as e:
        logger.error(f"Failed to extract queries from {file_path}: {str(e)}")
        return []


def create_query_batch_configs(
    unique_queries: List[str],
    session_id: str,
    company_name: str,
    from_dialect: str,
    to_dialect: str,
    query_column: str,
    batch_size: int,
    file_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Create batch configurations with pre-loaded queries
    Uses hash-based distribution for consistent batching
    """
    if not unique_queries:
        return []
    
    # Calculate number of batches needed
    num_batches = max(1, (len(unique_queries) + batch_size - 1) // batch_size)
    
    # Hash-based distribution (same logic as worker)
    def sha256_hash_to_batch(query_text, num_batches):
        hash_bytes = hashlib.sha256(str(query_text).encode('utf-8')).digest()
        hash_int = int.from_bytes(hash_bytes[:8], byteorder='big')
        return hash_int % num_batches
    
    # Group queries by batch
    batch_queries = [[] for _ in range(num_batches)]
    
    for query in unique_queries:
        batch_idx = sha256_hash_to_batch(query, num_batches)
        batch_queries[batch_idx].append(query)
    
    # Create configurations
    batch_configs = []
    
    for batch_idx in range(num_batches):
        if not batch_queries[batch_idx]:  # Skip empty batches
            continue
            
        batch_config = {
            'batch_id': f"batch_{session_id}_{batch_idx}",
            'session_id': session_id,
            'file_name': file_config['file_name'],
            'batch_idx': batch_idx,
            'total_batches': num_batches,
            'company_name': company_name,
            'from_dialect': from_dialect,
            'to_dialect': to_dialect,
            'query_column': query_column,
            'queries': batch_queries[batch_idx],  # Actual query list
            'query_count': len(batch_queries[batch_idx])
        }
        
        batch_configs.append(batch_config)
    
    logger.info(f"Created {len(batch_configs)} batches from {len(unique_queries)} queries")
    return batch_configs


def orchestrate_processing(
    directory_path: str,
    company_name: str,
    from_dialect: str,
    to_dialect: str,
    query_column: str,
    batch_size: int = 10000,
    filters: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Orchestrate the entire processing pipeline using Celery with optimized file reading
    NEW APPROACH: Read each file once in orchestrator, distribute queries to workers
    
    Returns immediately with task group ID that can be monitored
    Following TestDriven.io pattern - let Celery handle all the complexity
    """
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
        
        # NEW APPROACH: Read files and extract queries in orchestrator
        all_batch_configs = []
        
        for file_config in file_configs:
            logger.info(f"📖 Reading and processing file: {file_config['file_name']}")
            
            # Extract unique queries from this file
            unique_queries = extract_unique_queries_from_file(
                file_config['file_path'],
                query_column,
                filters or {}
            )
            
            if not unique_queries:
                logger.warning(f"No queries found in {file_config['file_name']}")
                continue
            
            logger.info(f"✅ Extracted {len(unique_queries):,} unique queries from {file_config['file_name']}")
            
            # Create batch configurations with actual queries
            file_batch_configs = create_query_batch_configs(
                unique_queries,
                session_id,
                company_name,
                from_dialect,
                to_dialect,
                query_column,
                batch_size,
                file_config
            )
            
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
        
        logger.info(f"✅ Launched {len(all_batch_configs)} tasks for processing")
        
        # Return immediately with tracking information
        return {
            'session_id': session_id,
            'group_id': group_result.id if hasattr(group_result, 'id') else session_id,
            'status': 'processing',
            'total_files': len(file_configs),
            'total_batches': len(all_batch_configs),
            'task_ids': [f"{session_id}_{config['batch_id']}" for config in all_batch_configs],
            'created_at': datetime.now().isoformat(),
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
    """
    from celery.result import AsyncResult
    import redis
    
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
    
    return {
        'session_id': session_id,
        'total_tasks': total_tasks,
        'total_batches': total_batches,  # Add this for clarity
        'completed': completed,
        'failed': failed,
        'pending': pending,
        'processing': processing,
        'successful_batches': successful_batches,  # Add this field
        'progress_percentage': round(progress_percentage, 1),
        'task_details': results[:10],  # Limit to first 10 for response size
        'overall_status': 'completed' if completed + failed >= total_tasks else 'processing'
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
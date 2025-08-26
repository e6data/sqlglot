"""
Task Management for Automated Processing
Handles task creation and orchestration following TestDriven.io patterns
Supports both local files and S3 paths with temporary credentials
"""
import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
import pyarrow.fs as fs

# Minimal logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)



def get_filesystem(path: str) -> Optional[fs.FileSystem]:
    """
    Get appropriate filesystem based on path (S3 or local)
    """
    if path.startswith('s3://'):
        # Hardcoded temporary AWS credentials
        s3fs = fs.S3FileSystem(
            access_key="ASIAZYHN7XI673KNYGXA",
            secret_key="BdclzsbEuxCsICxrIfoJdFzWU97VM1P/3TNJnYWk",
            session_token="FwoGZXIvYXdzEBcaDBDLPmDnwH1dNxLi4CLWAdkb9Gqe2Y0RZglxCIgvESmp3+jK2nJYHKT0WQyXdMamsJr8hC6go+wee5yIcAJxw7N5YTEHTpdYGSsmABKwsithjPWaUCD2arFobAiQOxSUgiUdGh8q1+HvtR7OkNSra8HqKmgHurbE12WvgxGYqkAD1ZRlIDQVYpY+loUNmHxav/Qc5TeKvMWxm3p/EhFNuefB1+eZeoReQ3D/c4WQ4ooxJ7aqtStoGj3BsZtdkmIqwsMg4d+jMNhfMtKTwKlylegjRMGJ+bzRw0yxnvYkmf9EnsILzK0o5r6xxQYyM0LjyD4fdNO+2b349x0i9se/dd709WnZ2EDCnWGsLwWr2Qjedg8X28KlOiPJWKf/JgUwEQ==",
            region="us-east-1",
            connect_timeout=30,
            request_timeout=60
        )
        return s3fs
    return None

def discover_parquet_files(
    directory_path: str,
    query_column: str
) -> List[Dict[str, Any]]:
    """
    Discover and validate parquet files in directory using metadata only
    Supports both local filesystem and S3 with temporary credentials
    Memory-efficient: only reads schema and row count metadata, not actual data
    """
    valid_files = []
    
    if directory_path.startswith('s3://'):
        # S3 path handling
        filesystem = get_filesystem(directory_path)
        bucket_and_key = directory_path.replace('s3://', '')
        
        # Remove trailing slash if present
        if bucket_and_key.endswith('/'):
            bucket_and_key = bucket_and_key.rstrip('/')
        
        # Check if the path is a single file or a directory
        import pyarrow.fs as fs
        file_info = filesystem.get_file_info(bucket_and_key)
        
        parquet_files = []
        
        if file_info.type == fs.FileType.File:
            # Single file provided - process it directly
            if bucket_and_key.endswith('.parquet'):
                parquet_files = [bucket_and_key]
            else:
                raise ValueError(f"Path is not a parquet file: {directory_path}")
        else:
            # Directory provided - list files in S3
            from pyarrow.fs import FileSelector
            selector = FileSelector(bucket_and_key, recursive=False)
            file_infos = filesystem.get_file_info(selector)
            
            for finfo in file_infos:
                if finfo.type == fs.FileType.File and finfo.path.endswith('.parquet'):
                    parquet_files.append(finfo.path)
        
        if not parquet_files:
            raise ValueError(f"No parquet files found in {directory_path}")
        
        # Process S3 files
        for s3_path in parquet_files:
            try:
                import pyarrow.parquet as pq
                # Read metadata from S3
                parquet_file = pq.ParquetFile(s3_path, filesystem=filesystem)
                
                # Get schema from metadata
                schema = parquet_file.schema
                columns = [field.name for field in schema]
                
                # Check if required columns exist
                if query_column not in columns:
                    logger.warning(f"Skipping {s3_path}: missing column '{query_column}'")
                    continue
                
                # Get row count from metadata (no data loading)
                row_count = parquet_file.metadata.num_rows
                file_size_mb = 0  # S3 file size not easily available
                
                valid_files.append({
                    'file_path': f's3://{s3_path}',  # Keep S3 prefix
                    'file_name': os.path.basename(s3_path),
                    'row_count': row_count,
                    'file_size_mb': file_size_mb,
                    'columns': columns,
                    'schema': schema
                })
                
                logger.info(f"Validated {s3_path}: {row_count:,} rows")
                
            except Exception as e:
                logger.error(f"Failed to validate {s3_path}: {str(e)}")
                continue
    else:
        # Local filesystem handling
        directory = Path(directory_path)
        if not directory.exists():
            raise ValueError(f"Directory not found: {directory_path}")
        
        parquet_files = list(directory.glob("*.parquet"))
        if not parquet_files:
            raise ValueError(f"No parquet files found in {directory_path}")
        
        # Process local files (existing code)
        for file_path in parquet_files:
            try:
                # Read only metadata - no actual data loading
                import pyarrow.parquet as pq
                parquet_file = pq.ParquetFile(str(file_path))
                
                # Get schema from metadata
                schema = parquet_file.schema
                columns = [field.name for field in schema]
                
                # Check if required columns exist
                if query_column not in columns:
                    logger.warning(f"Skipping {file_path.name}: missing column '{query_column}'")
                    continue
                
                # Get row count from metadata (no data loading)
                row_count = parquet_file.metadata.num_rows
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                
                valid_files.append({
                    'file_path': str(file_path),
                    'file_name': file_path.name,
                    'row_count': row_count,
                    'file_size_mb': round(file_size_mb, 2),
                    'columns': columns,
                    'schema': schema  # Include schema for later use
                })
                
                logger.info(f"Validated {file_path.name}: {row_count:,} rows ({file_size_mb:.1f} MB)")
                
            except Exception as e:
                logger.error(f"Failed to validate {file_path.name}: {str(e)}")
                continue
    
    return valid_files


def create_batch_configs(
    file_configs: List[Dict],
    session_id: str,
    company_name: str,
    from_dialect: str,
    to_dialect: str,
    query_column: str,
    batch_size: int,
    filters: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    """
    Create batch configurations for processing
    Memory-efficient: uses metadata-based estimation instead of loading full data
    """
    batch_configs = []
    global_batch_id = 0
    
    for file_config in file_configs:
        file_path = file_config['file_path']
        
        # Use metadata-based estimation instead of loading full data
        try:
            # Get available columns from previously stored schema
            available_columns = file_config['columns']
            
            # Validate filter columns exist
            filter_columns = []
            if filters:
                for filter_col in filters.keys():
                    if filter_col in available_columns:
                        filter_columns.append(filter_col)
                    else:
                        logger.warning(f"Filter column '{filter_col}' not found in {file_config['file_name']}")
            
            # Use metadata-based estimation for unique queries
            # Conservative estimate: assume 70% of rows are unique after filtering
            total_rows = file_config['row_count']
            
            # Apply rough filter estimation (if we had real statistics we'd be more precise)
            if filters:
                # Estimate filtered rows (conservative: 30% remain after filtering)
                estimated_filtered_rows = int(total_rows * 0.3)
            else:
                estimated_filtered_rows = total_rows
            
            # Estimate unique queries (conservative: 70% are unique)
            estimated_unique = int(estimated_filtered_rows * 0.7)
            
            logger.info(f"File {file_config['file_name']}: estimated {estimated_unique:,} unique queries from {total_rows:,} total rows")
            
        except Exception as e:
            logger.warning(f"Using fallback estimation for {file_config['file_name']}: {str(e)}")
            # Ultra-conservative fallback
            estimated_unique = min(file_config['row_count'] // 3, 100000)
        
        # Calculate number of batches based on estimated unique count
        num_batches = max(1, (estimated_unique + batch_size - 1) // batch_size)
        
        # Create batch config for each batch
        for batch_idx in range(num_batches):
            batch_config = {
                'batch_id': f"batch_{global_batch_id}",
                'session_id': session_id,
                'file_path': file_config['file_path'],
                'file_name': file_config['file_name'],
                'file_size_mb': file_config.get('file_size_mb', 0),
                'batch_idx': batch_idx,
                'total_batches': num_batches,
                'company_name': company_name,
                'from_dialect': from_dialect,
                'to_dialect': to_dialect,
                'query_column': query_column,
                'batch_size': batch_size,
                'estimated_unique_queries': estimated_unique,  # Estimated count
                'available_columns': available_columns,  # Pass column list
                'filters': filters or {}  # Pass filters to workers
            }
            
            batch_configs.append(batch_config)
            global_batch_id += 1
    
    logger.info(f"Created {len(batch_configs)} batch configurations across {len(file_configs)} files")
    
    return batch_configs


def store_session_metadata(session_id: str, metadata: Dict[str, Any]):
    """
    Store session metadata in Redis for tracking
    """
    try:
        import redis
        client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        import json
        key = f"session:{session_id}"
        client.hset(key, mapping={
            'metadata': json.dumps(metadata)
        })
        client.expire(key, 86400)  # 24 hour expiry
        
        logger.info(f"Stored metadata for session {session_id}")
        
    except Exception as e:
        logger.error(f"Failed to store session metadata: {str(e)}")


def get_task_status(task_id: str) -> Dict[str, Any]:
    """
    Get status of a processing task
    Following TestDriven.io pattern for task status checking
    """
    from celery.result import AsyncResult
    
    result = AsyncResult(task_id)
    
    if result.state == 'PENDING':
        return {
            'task_id': task_id,
            'state': 'PENDING',
            'current': 0,
            'total': 100,
            'status': 'Task waiting to be processed...'
        }
    elif result.state == 'PROGRESS':
        return {
            'task_id': task_id,
            'state': 'PROGRESS',
            'current': result.info.get('current', 0),
            'total': result.info.get('total', 100),
            'status': result.info.get('status', 'Processing...')
        }
    elif result.state == 'SUCCESS':
        return {
            'task_id': task_id,
            'state': 'SUCCESS',
            'result': result.result,
            'status': 'Task completed successfully'
        }
    elif result.state == 'FAILURE':
        return {
            'task_id': task_id,
            'state': 'FAILURE',
            'error': str(result.info),
            'status': 'Task failed'
        }
    else:
        return {
            'task_id': task_id,
            'state': result.state,
            'status': f'Task in state: {result.state}'
        }
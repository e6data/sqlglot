"""
Task Management for Automated Processing
Handles task creation and orchestration following TestDriven.io patterns
Supports both local files and S3 paths with temporary credentials
"""
import logging
import os
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional
import pyarrow as pa
import pyarrow.fs as fs

# Minimal logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)



def get_filesystem(path: str) -> Optional[fs.FileSystem]:
    """
    Get appropriate filesystem based on path (S3 or local)
    """
    if path.startswith('s3://'):
        # Updated AWS credentials
        s3fs = fs.S3FileSystem(
            access_key="ASIAZYHN7XI64V6RB3JE",
            secret_key="ivFKpPAYVeLxKVAHzwBm5UvUw95jI2eOuXoWop5t",
            session_token="FwoGZXIvYXdzEFYaDJYO/Msc2RGRhHkyNCLWAVEJ/q5S2bfCV6fYnnOO8AbEP0PdPyEKpE5xxFiJ2CC8ocmffBUUf59VUk0JQiEbljmqsyg7aOUkwm4zHUk4NYidd/2fSakcuawYV0QnL6ZbKMOjPN1wlCaXJYsDPXCvcuGXKP5FWXvJsmLcrLG0YQeLzC3DWfxjacAPinZAKOKrA/YkzXwVslYqM+hDK+fjqwiVK3BHFFXn4kUkI3uBrtJW94hueIG5dvSMYL4C7A/7I9wHLIC+zVEYCd3Tch95X1x8K+VBt4ayFdtiaAHY0oJ6K+zhTWEok8K/xQYyM84wjGOZFVNzChrNGcUhY1ph1KmVh5kYc58relyWJ992BU0WdNNW4T9VuFttIbwxnbv6Kw==",
            region="us-east-1",
            connect_timeout=30,
            request_timeout=60
        )
        return s3fs
    return None

def discover_parquet_files(
    directory_path: str,
    query_column: str
) -> List[str]:
    """
    Simplified parquet file discovery
    Step 1: Check S3 vs local path
    Step 2: Check if single file or directory  
    Step 3: Add all parquet files to list
    """
    parquet_files = []
    
    # Step 1: Check whether it's S3 or local path
    if directory_path.startswith('s3://'):
        # S3 path
        filesystem = get_filesystem(directory_path)
        bucket_and_key = directory_path.replace('s3://', '').rstrip('/')
        
        # Step 2: Check if single file or directory
        import pyarrow.fs as fs
        file_info = filesystem.get_file_info(bucket_and_key)
        
        if file_info.type == fs.FileType.File:
            # Single parquet file
            if bucket_and_key.endswith('.parquet'):
                parquet_files = [f's3://{bucket_and_key}']
        else:
            # Step 3: Directory - add all parquet files
            from pyarrow.fs import FileSelector
            selector = FileSelector(bucket_and_key, recursive=False)
            file_infos = filesystem.get_file_info(selector)
            
            for finfo in file_infos:
                if finfo.type == fs.FileType.File and finfo.path.endswith('.parquet'):
                    parquet_files.append(f's3://{finfo.path}')
    else:
        # Local path
        path = Path(directory_path)
        
        # Step 2: Check if single file or directory
        if path.is_file():
            # Single parquet file
            if path.suffix.lower() == '.parquet':
                parquet_files = [str(path)]
        elif path.is_dir():
            # Step 3: Directory - add all parquet files
            parquet_files = [str(p) for p in path.glob("*.parquet")]
    
    return parquet_files


def extract_unique_queries_from_file(
    file_path: str, 
    query_column: str, 
    filters: Dict[str, Any]
) -> pa.Table:
    """
    Step 1: Read only query column, get unique queries
    Step 2: Push filters using PyArrow operations
    """
    try:
        import pyarrow.dataset as ds
        import s3fs
        
        logger.info(f"Reading file: {file_path}")
        
        # Create dataset
        if file_path.startswith('s3://'):
            s3fs_fs = s3fs.S3FileSystem(
                key="ASIAZYHN7XI64V6RB3JE",
                secret="ivFKpPAYVeLxKVAHzwBm5UvUw95jI2eOuXoWop5t",
                token="FwoGZXIvYXdzEFYaDJYO/Msc2RGRhHkyNCLWAVEJ/q5S2bfCV6fYnnOO8AbEP0PdPyEKpE5xxFiJ2CC8ocmffBUUf59VUk0JQiEbljmqsyg7aOUkwm4zHUk4NYidd/2fSakcuawYV0QnL6ZbKMOjPN1wlCaXJYsDPXCvcuGXKP5FWXvJsmLcrLG0YQeLzC3DWfxjacAPinZAKOKrA/YkzXwVslYqM+hDK+fjqwiVK3BHFFXn4kUkI3uBrtJW94hueIG5dvSMYL4C7A/7I9wHLIC+zVEYCd3Tch95X1x8K+VBt4ayFdtiaAHY0oJ6K+zhTWEok8K/xQYyM84wjGOZFVNzChrNGcUhY1ph1KmVh5kYc58relyWJ992BU0WdNNW4T9VuFttIbwxnbv6Kw==",
                client_kwargs={'region_name': 'us-east-1'},
                config_kwargs={'connect_timeout': 30, 'read_timeout': 60}
            )
            dataset = ds.dataset(file_path, filesystem=s3fs_fs)
        else:
            dataset = ds.dataset(file_path)
        
        # Step 1: Read only query column (ignore filters), get unique queries
        query_table = dataset.to_table(columns=[query_column])
        logger.info(f"Loaded {len(query_table)} rows, getting unique queries")
        unique_queries_table = query_table.group_by([query_column]).aggregate([])
        logger.info(f"Found {len(unique_queries_table)} unique queries")
        
        # Step 2: If filters exist, push them using PyArrow operations
        if filters:
            logger.info(f"Applying filters: {list(filters.keys())}")
            # Build filter expressions for the original dataset
            filter_expressions = []
            for filter_col, filter_value in filters.items():
                if isinstance(filter_value, list) and len(filter_value) > 0:
                    filter_expressions.append(ds.field(filter_col).isin(filter_value))
                elif not isinstance(filter_value, list):
                    filter_expressions.append(ds.field(filter_col) == filter_value)
            
            if filter_expressions:
                # Combine all filters
                combined_filter = filter_expressions[0]
                for expr in filter_expressions[1:]:
                    combined_filter = combined_filter & expr
                
                # Apply filters and get unique queries from filtered data
                filtered_table = dataset.to_table(filter=combined_filter, columns=[query_column])
                unique_queries_table = filtered_table.group_by([query_column]).aggregate([])
                logger.info(f"After filtering: {len(unique_queries_table)} unique queries")
        
        return unique_queries_table
        
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
) -> tuple[pa.Table, Dict[str, Any]]:
    """
    Initialize PyArrow table first, then append to query arrays directly
    """
    if len(unique_table) == 0:
        logger.warning("Empty unique_table provided")
        return pa.table({}), {}
    
    # Calculate num of batches
    num_queries = len(unique_table)
    num_batches = max(1, (num_queries + batch_size - 1) // batch_size)
    logger.info(f"Creating {num_batches} batches from {num_queries} queries")
    
    # Initialize the PyArrow table first
    batch_ids = list(range(num_batches))
    empty_arrays = [[] for _ in range(num_batches)]
    
    logger.info(f"Initializing PyArrow table with {num_batches} empty batch arrays")
    batch_table = pa.table({
        'batch_id': pa.array(batch_ids, type=pa.int32()),
        'queries_array': pa.array(empty_arrays, type=pa.list_(pa.string()))
    })
    
    # Get the queries arrays to modify
    queries_arrays = batch_table['queries_array'].to_pylist()
    
    # Iterate through queries and hash
    query_list = unique_table[query_column].to_pylist()
    logger.info(f"Processing {len(query_list)} queries with hash-based distribution")
    
    for query in query_list:
        # Get batch_id using hashing (EXACT SAME LOGIC)
        hash_obj = hashlib.sha256(query.encode('utf-8'))
        hash_int = int.from_bytes(hash_obj.digest()[:8], 'big')
        batch_id = hash_int % num_batches
        
        # Append to query array in the arrow table
        queries_arrays[batch_id].append(query)
    
    # Update the table with modified arrays
    batch_table = pa.table({
        'batch_id': pa.array(batch_ids, type=pa.int32()),
        'queries_array': pa.array(queries_arrays, type=pa.list_(pa.string()))
    })
    
    # Log batch distribution
    batch_sizes = [len(arr) for arr in queries_arrays]
    logger.info(f"Batch distribution: {dict(zip(batch_ids, batch_sizes))}")
    
    # Create metadata
    metadata = {
        'session_id': session_id,
        'company_name': company_name,
        'from_dialect': from_dialect,
        'to_dialect': to_dialect,
        'query_column': query_column,
        'batch_size': batch_size,
        'file_config': file_config,
        'total_queries': num_queries,
        'total_batches': num_batches
    }
    
    logger.info(f"Created PyArrow table with {len(batch_table)} batches")
    return batch_table, metadata
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
        path = Path(directory_path)
        if not path.exists():
            raise ValueError(f"Path not found: {directory_path}")
        
        parquet_files = []
        
        if path.is_file():
            # Single file provided - process it directly
            if path.suffix.lower() == '.parquet':
                parquet_files = [path]
            else:
                raise ValueError(f"Path is not a parquet file: {directory_path}")
        elif path.is_dir():
            # Directory provided - list parquet files
            parquet_files = list(path.glob("*.parquet"))
            if not parquet_files:
                raise ValueError(f"No parquet files found in {directory_path}")
        else:
            raise ValueError(f"Invalid path (not a file or directory): {directory_path}")
        
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
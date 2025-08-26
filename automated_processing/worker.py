"""
Celery Worker for Automated Processing
Following patterns from TestDriven.io FastAPI+Celery guide
Run with: celery -A worker.celery worker --loglevel=info
"""
from celery import Celery
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa
import numpy as np
import logging
import sys
import os
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from tqdm import tqdm
import s3fs

# Minimal logging setup
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Celery Configuration (embedded for now)
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# Create Celery app - this will be accessible as worker.celery
celery = Celery(
    'automated_processing',
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND
)

# Configure Celery for optimal autoscaling
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,
    task_soft_time_limit=3300,
    # Autoscaling optimization settings
    worker_prefetch_multiplier=1,  # Only prefetch 1 task per worker (better for autoscaling)
    task_acks_late=True,          # Acknowledge after task completion (safer)
    worker_max_tasks_per_child=5, # Restart workers after 5 tasks (prevent S3 memory issues)
    worker_disable_rate_limits=True, # Better performance for CPU-bound tasks
    result_expires=86400,
    task_routes={
        'process_batch': {'queue': 'processing_queue'},
        'process_query_batch': {'queue': 'processing_queue'}
    }
)

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)


@celery.task(bind=True, name='process_query_batch', max_retries=3)
def process_query_batch(self, batch_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a pre-loaded list of queries (NEW OPTIMIZED APPROACH)
    No file reading needed - queries are provided directly from orchestrator
    """
    session_id = batch_data['session_id']
    batch_id = batch_data['batch_id']
    queries = batch_data['queries']  # Pre-loaded query list
    
    worker_id = f"{self.request.hostname}:{self.request.id[:8]}"
    
    try:
        # Process the pre-loaded queries directly
        result = process_query_list(batch_data, self)
        
        # Store to Iceberg
        iceberg_success = store_batch_to_iceberg(result['queries'], batch_data)
        
        result['iceberg_success'] = iceberg_success
        result['worker_id'] = worker_id
        result['completed_at'] = datetime.now().isoformat()
        
        return result
        
    except Exception as exc:
        # Retry with exponential backoff (TestDriven.io pattern)
        if self.request.retries < self.max_retries:
            retry_delay = 60 * (2 ** self.request.retries)
            raise self.retry(exc=exc, countdown=retry_delay)
        else:
            return {
                'batch_id': batch_id,
                'status': 'failed',
                'error': str(exc),
                'worker_id': worker_id
            }


# S3 credential helpers (used by both old and new approaches)
def get_s3_filesystem_credentials() -> Dict[str, str]:
    """Return S3 credentials for s3fs filesystem creation"""
    return {
        "access_key": "ASIAZYHN7XI6UIUBDZKV",
        "secret_key": "Je9sw4Vg+i4WBzRQQD6lYPnwo6o/jxIOorSQhosu", 
        "session_token": "FwoGZXIvYXdzEBAaDHaGtaOADz6xDXuDbSLWAQ5qTNEw+lcm9CfpOE3oGaFOE0Gsmc0qzD4NFZIcVzt3e3B8MScMq1mhP6mp3//xDJSqeK+3oPDwdsc2iAik0xga9yloWwjU1Wvzvi+GWNrNCOcFLbkdPZ+dvlC8KrFlyE61ZozQrFDK/1V8+ipYHfNXPOX2MYeJKEZ0+pKe7Ij6hh7qlG2rYl3uX5+6WKjnjw9ez1+VQ7IFTnpgV87EEDm/5SmEjJo9AhXf0aUgBDdbsV5UvUuVcncnfwHAK1TK+P8Mu5wio4GVmCYtRl8HJ5eP236GqRAojPuvxQYyM3o8RsOb0KjLEX9kjogwpiZ4D6pKZbbsTr02nzpZML4LUzXRUEdqra3nCYmKeoTvh5vCpg==",
        "region": "us-east-1"
    }

def create_s3fs_filesystem_in_task(credentials: Dict[str, str]) -> s3fs.S3FileSystem:
    """Create s3fs filesystem inside task (multiprocessing-friendly)"""
    try:
        s3fs_fs = s3fs.S3FileSystem(
            key=credentials["access_key"],
            secret=credentials["secret_key"], 
            token=credentials["session_token"],
            client_kwargs={'region_name': credentials["region"]},
            config_kwargs={'connect_timeout': 30, 'read_timeout': 60}
        )
        return s3fs_fs
    except Exception as e:
        logger.error(f"❌ Failed to create s3fs filesystem: {str(e)}")
        raise

def process_batch_queries(batch_data: Dict[str, Any], task_instance=None) -> Dict[str, Any]:
    """
    Process queries from parquet file using memory-efficient selective column loading
    Supports both local files and S3 paths with temporary credentials
    Only loads required columns and filters data before deduplication
    """
    file_path = batch_data['file_path']
    query_column = batch_data['query_column']
    batch_idx = batch_data.get('batch_idx', 0)
    total_batches = batch_data.get('total_batches', 1)
    filters = batch_data.get('filters', {})
    available_columns = batch_data.get('available_columns', [])
    
    # Import statistics function from local module
    from automated_processing.statistics import analyze_sql_functions
    
    # Selective column loading - only load what we need
    columns_to_read = [query_column]  # Always need the query column
    
    # Only add filter columns that actually exist and are needed
    active_filter_columns = []
    if filters:
        for filter_col, filter_value in filters.items():
            if filter_col in available_columns:
                active_filter_columns.append(filter_col)
                columns_to_read.append(filter_col)
    
    logger.info(f"Loading only columns: {columns_to_read} from {file_path}")
    
    # Create PyArrow dataset with filesystem support
    # Use direct parquet reading to avoid dataset API issues
    try:
        if file_path.startswith('s3://'):
            # S3 path - use s3fs (multiprocessing-friendly) with PyArrow
            import pyarrow.parquet as pq
            
            credentials = get_s3_filesystem_credentials()
            s3fs_fs = create_s3fs_filesystem_in_task(credentials)
            
            logger.info(f"📖 Reading parquet from S3: {file_path}")
            
            # Read from S3 using s3fs filesystem with PyArrow
            table = pq.read_table(
                file_path,
                filesystem=s3fs_fs,
                columns=columns_to_read
            )
        else:
            # Local path - use dataset API
            logger.info(f"Creating dataset from local path: {file_path}")
            dataset = ds.dataset(file_path, format="parquet")
            table = dataset.to_table(columns=columns_to_read)
    except Exception as e:
        logger.error(f"Failed to read parquet file: {str(e)}")
        raise ValueError(f"Cannot access parquet file: {str(e)}")
    
    # Apply filters to the loaded table
    logger.info(f"Table loaded with {len(table):,} rows")
    
    # Apply filters using PyArrow compute
    filter_conditions = []
    filter_conditions.append(pc.is_valid(table[query_column]))
    filter_conditions.append(pc.not_equal(table[query_column], ""))
    
    for filter_col, filter_value in filters.items():
        if filter_col in active_filter_columns:
            filter_conditions.append(pc.equal(table[filter_col], filter_value))
    
    if filter_conditions:
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = pc.and_kleene(combined_filter, condition)
        
        table = table.filter(combined_filter)
        logger.info(f"After filtering: {len(table):,} rows")
    
    # Early exit if no data after filtering
    if len(table) == 0:
        logger.info(f"No data remaining after filtering for batch {batch_idx}")
        return {
            'batch_id': batch_data['batch_id'],
            'processed_count': 0,
            'successful_count': 0,
            'queries': [],
            'status': 'completed'
        }
    
    # Deduplicate using only query column
    unique_table = table.select([query_column]).group_by([query_column]).aggregate([])
    logger.info(f"Deduplicated to {len(unique_table):,} unique queries")
    
    # Cast to large_string to prevent overflow for long queries
    for i, field in enumerate(unique_table.schema):
        if field.type == pa.string():
            unique_table = unique_table.set_column(
                i, field.name,
                pc.cast(unique_table.column(i), pa.large_string())
            )
    
    # Hash-based SHA256 distribution for consistent batch assignment
    query_array = unique_table[query_column].to_numpy(zero_copy_only=False)
    
    def sha256_hash_to_batch(query_text, num_batches):
        hash_bytes = hashlib.sha256(str(query_text).encode('utf-8')).digest()
        hash_int = int.from_bytes(hash_bytes[:8], byteorder='big')
        return hash_int % num_batches
    
    # Vectorized hash calculation
    hash_values = np.array([sha256_hash_to_batch(x, total_batches) for x in query_array])
    batch_mask = (hash_values == batch_idx)
    batch_indices = np.where(batch_mask)[0]
    
    # Get queries for this specific batch
    batch_table = unique_table.take(batch_indices)
    queries = batch_table[query_column].to_pylist()
    
    logger.info(f"Processing {len(queries):,} queries for batch {batch_idx}/{total_batches-1}")
    
    # Process queries with progress tracking
    results = []
    from_dialect = batch_data.get('from_dialect', 'snowflake')
    to_dialect = batch_data.get('to_dialect', 'e6')
    
    progress_desc = f"Batch {batch_idx}/{total_batches-1}"
    for i, query_text in enumerate(tqdm(queries, desc=progress_desc, unit="queries")):
        query_id = f"batch_{batch_idx}_query_{i+1}"
        
        try:
            analysis = analyze_sql_functions(
                query=query_text,
                from_sql=from_dialect,
                query_id=query_id,
                to_sql=to_dialect
            )
            
            results.append({
                'query_id': query_id,
                'status': 'success' if not analysis.get('error') else 'failed',
                'original_query': query_text,
                'converted_query': analysis.get('converted-query', ''),
                'executable': analysis.get('executable', 'NO'),
                'supported_functions': analysis.get('supported_functions', []),
                'unsupported_functions': list(set(analysis.get('unsupported_functions', []))),
                'processing_time_ms': 100
            })
            
        except Exception as e:
            results.append({
                'query_id': query_id,
                'status': 'failed',
                'original_query': query_text,
                'error': str(e)
            })
    
    return {
        'batch_id': batch_data['batch_id'],
        'processed_count': len(results),
        'successful_count': len([r for r in results if r['status'] == 'success']),
        'queries': results,
        'status': 'completed'
    }


def process_query_list(batch_data: Dict[str, Any], task_instance=None) -> Dict[str, Any]:
    """
    Process pre-loaded list of queries (NEW OPTIMIZED APPROACH)
    No file reading needed - queries provided directly from orchestrator
    """
    queries = batch_data['queries']  # Pre-loaded query list
    batch_idx = batch_data.get('batch_idx', 0)
    total_batches = batch_data.get('total_batches', 1)
    
    # Import statistics function from local module
    from automated_processing.statistics import analyze_sql_functions
    
    logger.info(f"Processing {len(queries):,} pre-loaded queries for batch {batch_idx}/{total_batches-1}")
    
    # Process queries with progress tracking
    results = []
    from_dialect = batch_data.get('from_dialect', 'snowflake')
    to_dialect = batch_data.get('to_dialect', 'e6')
    
    progress_desc = f"Batch {batch_idx}/{total_batches-1}"
    for i, query_text in enumerate(tqdm(queries, desc=progress_desc, unit="queries")):
        query_id = f"batch_{batch_idx}_query_{i+1}"
        
        try:
            analysis = analyze_sql_functions(
                query=query_text,
                from_sql=from_dialect,
                query_id=query_id,
                to_sql=to_dialect
            )
            
            results.append({
                'query_id': query_id,
                'status': 'success' if not analysis.get('error') else 'failed',
                'original_query': query_text,
                'converted_query': analysis.get('converted-query', ''),
                'executable': analysis.get('executable', 'NO'),
                'supported_functions': analysis.get('supported_functions', []),
                'unsupported_functions': list(set(analysis.get('unsupported_functions', []))),
                'processing_time_ms': 100
            })
            
        except Exception as e:
            results.append({
                'query_id': query_id,
                'status': 'failed',
                'original_query': query_text,
                'error': str(e)
            })
    
    return {
        'batch_id': batch_data['batch_id'],
        'processed_count': len(results),
        'successful_count': len([r for r in results if r['status'] == 'success']),
        'queries': results,
        'status': 'completed'
    }


def store_batch_to_iceberg(queries: List[Dict], batch_data: Dict[str, Any]) -> bool:
    """
    Store batch results to existing batch_statistics Iceberg table
    """
    if not queries:
        return True
    
    try:
        from automated_processing.iceberg_handler import iceberg_catalog
        
        if not iceberg_catalog:
            logger.error("Iceberg catalog not available")
            return False
        
        # Load existing batch_statistics table
        table = iceberg_catalog.load_table("default.batch_statistics")
        
        # Prepare data for PyArrow table
        current_time = datetime.now()
        event_date = current_time.strftime("%Y-%m-%d")
        
        # Create arrays for each column
        data = {
            "query_id": [],
            "batch_id": [],
            "company_name": [],
            "event_date": [],
            "batch_number": [],
            "timestamp": [],
            "status": [],
            "executable": [],
            "from_dialect": [],
            "to_dialect": [],
            "original_query": [],
            "converted_query": [],
            "supported_functions": [],
            "unsupported_functions": [],
            "udf_list": [],
            "tables_list": [],
            "processing_time_ms": [],
            "error_message": []
        }
        
        for i, query in enumerate(queries):
            data["query_id"].append(i + 1)
            data["batch_id"].append(f"{batch_data['session_id']}_{batch_data['batch_id']}")
            data["company_name"].append(batch_data.get('company_name', 'unknown'))
            data["event_date"].append(event_date)
            data["batch_number"].append(batch_data.get('batch_idx', 0))
            data["timestamp"].append(current_time)
            data["status"].append(query.get('status', 'unknown'))
            data["executable"].append(query.get('executable', 'NO'))
            data["from_dialect"].append(batch_data.get('from_dialect', ''))
            data["to_dialect"].append(batch_data.get('to_dialect', ''))
            data["original_query"].append(query.get('original_query', ''))
            data["converted_query"].append(query.get('converted_query', ''))
            data["supported_functions"].append(query.get('supported_functions', []))
            data["unsupported_functions"].append(query.get('unsupported_functions', []))
            data["udf_list"].append([])  # Empty for now
            data["tables_list"].append([])  # Empty for now
            data["processing_time_ms"].append(query.get('processing_time_ms', 0))
            data["error_message"].append(query.get('error', ''))
        
        # Create PyArrow table
        arrow_table = pa.table({
            "query_id": pa.array(data["query_id"], type=pa.int64()),
            "batch_id": pa.array(data["batch_id"], type=pa.string()),
            "company_name": pa.array(data["company_name"], type=pa.string()),
            "event_date": pa.array(data["event_date"], type=pa.string()),
            "batch_number": pa.array(data["batch_number"], type=pa.int32()),
            "timestamp": pa.array(data["timestamp"], type=pa.timestamp('us')),
            "status": pa.array(data["status"], type=pa.string()),
            "executable": pa.array(data["executable"], type=pa.string()),
            "from_dialect": pa.array(data["from_dialect"], type=pa.string()),
            "to_dialect": pa.array(data["to_dialect"], type=pa.string()),
            "original_query": pa.array(data["original_query"], type=pa.string()),
            "converted_query": pa.array(data["converted_query"], type=pa.string()),
            "supported_functions": pa.array(data["supported_functions"], type=pa.list_(pa.string())),
            "unsupported_functions": pa.array(data["unsupported_functions"], type=pa.list_(pa.string())),
            "udf_list": pa.array(data["udf_list"], type=pa.list_(pa.string())),
            "tables_list": pa.array(data["tables_list"], type=pa.list_(pa.string())),
            "processing_time_ms": pa.array(data["processing_time_ms"], type=pa.int64()),
            "error_message": pa.array(data["error_message"], type=pa.string())
        })
        
        # Append to Iceberg table
        table.append(arrow_table)
        
        logger.info(f"Stored {len(queries)} results to Iceberg batch_statistics table")
        return True
        
    except Exception as e:
        logger.error(f"Failed to store to Iceberg: {str(e)}")
        return False

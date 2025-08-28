"""
Celery Worker for Automated Processing
Following patterns from TestDriven.io FastAPI+Celery guide
Run with: celery -A worker.celery worker --loglevel=info
"""
from celery import Celery
import pyarrow as pa
import pyarrow.compute as pc
import logging
import sys
import os
from datetime import datetime
from typing import Dict, Any
from tqdm import tqdm

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
    Consolidated function - Process PyArrow table of queries using vectorized operations
    Combines functionality from process_query_table, process_queries_vectorized, and process_chunk_pure_arrow
    """
    from automated_processing.statistics import analyze_sql_functions
    
    session_id = batch_data['session_id']
    batch_id = batch_data['batch_id']
    queries_list = batch_data.get('queries_table')  # Now a Python list
    query_column = batch_data.get('query_column', 'query')
    batch_idx = batch_data.get('batch_idx', 0)
    total_batches = batch_data.get('total_batches', 1)
    from_dialect = batch_data.get('from_dialect', 'snowflake')
    to_dialect = batch_data.get('to_dialect', 'e6')
    
    worker_id = f"{self.request.hostname}:{self.request.id[:8]}"
    
    try:
        num_queries = len(queries_list)
        logger.info(f"Processing {num_queries:,} queries in batch {batch_idx}/{total_batches-1}")
        
        # Convert list to PyArrow array for processing
        queries_array = pa.array(queries_list, type=pa.string())
        
        # Process in smaller chunks to maintain Arrow format
        chunk_size = 1000
        result_chunks = []
        
        progress_desc = f"Batch {batch_idx}/{total_batches-1}"
        
        for chunk_start in tqdm(range(0, num_queries, chunk_size), desc=progress_desc, unit="chunks"):
            chunk_end = min(chunk_start + chunk_size, num_queries)
            # Use PyArrow slice
            chunk_queries = queries_array[chunk_start:chunk_end]
            
            # Convert chunk to Python list only for SQL processing (unavoidable for SQLGlot)
            chunk_queries_list = chunk_queries.to_pylist()
            
            # Process all queries in chunk
            query_ids = []
            statuses = []
            original_queries = []
            converted_queries = []
            executables = []
            supported_functions_lists = []
            unsupported_functions_lists = []
            unsupported_functions_after_transpilation_lists = []
            joins_lists = []
            udf_lists = []
            tables_lists = []
            processing_times = []
            error_messages = []
            
            # Process queries (this part still needs Python iteration due to SQL analysis)
            for i, query_text in enumerate(chunk_queries_list):
                query_id = f"batch_{batch_idx}_query_{chunk_start + i + 1}"
                
                try:
                    analysis = analyze_sql_functions(
                        query=query_text,
                        from_sql=from_dialect,
                        query_id=query_id,
                        to_sql=to_dialect
                    )
                    
                    query_ids.append(query_id)
                    statuses.append('success' if not analysis.get('error') else 'failed')
                    original_queries.append(query_text)
                    converted_queries.append(analysis.get('converted-query', ''))
                    executables.append(analysis.get('executable', 'NO'))
                    supported_functions_lists.append(analysis.get('supported_functions', []))
                    unsupported_functions_lists.append(list(set(analysis.get('unsupported_functions', []))))
                    unsupported_functions_after_transpilation_lists.append(analysis.get('unsupported_functions_after_transpilation', []))
                    joins_lists.append(analysis.get('joins_list', []))
                    udf_lists.append(analysis.get('udf_list', []))
                    tables_lists.append(analysis.get('tables_list', []))
                    processing_times.append(100)
                    error_messages.append('')
                    
                except Exception as e:
                    query_ids.append(query_id)
                    statuses.append('failed')
                    original_queries.append(query_text)
                    converted_queries.append('')
                    executables.append('NO')
                    supported_functions_lists.append([])
                    unsupported_functions_lists.append([])
                    unsupported_functions_after_transpilation_lists.append([])
                    joins_lists.append([])
                    udf_lists.append([])
                    tables_lists.append([])
                    processing_times.append(0)
                    error_messages.append(str(e))
            
            # Create PyArrow table from results (vectorized)
            chunk_results = pa.table({
                'query_id': pa.array(query_ids, type=pa.string()),
                'status': pa.array(statuses, type=pa.string()),
                'original_query': pa.array(original_queries, type=pa.string()),
                'converted_query': pa.array(converted_queries, type=pa.string()),
                'executable': pa.array(executables, type=pa.string()),
                'supported_functions': pa.array(supported_functions_lists, type=pa.list_(pa.string())),
                'unsupported_functions': pa.array(unsupported_functions_lists, type=pa.list_(pa.string())),
                'unsupported_functions_after_transpilation': pa.array(unsupported_functions_after_transpilation_lists, type=pa.list_(pa.string())),
                'joins_list': pa.array(joins_lists, type=pa.list_(pa.string())),
                'udf_list': pa.array(udf_lists, type=pa.list_(pa.string())),
                'tables_list': pa.array(tables_lists, type=pa.list_(pa.string())),
                'processing_time_ms': pa.array(processing_times, type=pa.int64()),
                'error_message': pa.array(error_messages, type=pa.string())
            })
            result_chunks.append(chunk_results)
        
        # Concatenate all chunks using PyArrow (vectorized)
        if result_chunks:
            results_table = pa.concat_tables(result_chunks)
        else:
            # Empty table with correct schema
            results_table = pa.table({
                'query_id': pa.array([], type=pa.string()),
                'status': pa.array([], type=pa.string()),
                'original_query': pa.array([], type=pa.string()),
                'converted_query': pa.array([], type=pa.string()),
                'executable': pa.array([], type=pa.string()),
                'supported_functions': pa.array([], type=pa.list_(pa.string())),
                'unsupported_functions': pa.array([], type=pa.list_(pa.string())),
                'unsupported_functions_after_transpilation': pa.array([], type=pa.list_(pa.string())),
                'joins_list': pa.array([], type=pa.list_(pa.string())),
                'udf_list': pa.array([], type=pa.list_(pa.string())),
                'tables_list': pa.array([], type=pa.list_(pa.string())),
                'processing_time_ms': pa.array([], type=pa.int64()),
                'error_message': pa.array([], type=pa.string())
            })
        
        # Calculate success count using PyArrow compute
        success_mask = pc.equal(results_table['status'], 'success')
        successful_count = pc.sum(pc.cast(success_mask, pa.int64())).as_py()
        
        # Store to Iceberg using PyArrow table directly
        iceberg_success = store_results_table_to_iceberg(results_table, batch_data)
        
        return {
            'batch_id': batch_data['batch_id'],
            'processed_count': num_queries,
            'successful_count': successful_count,
            'iceberg_success': iceberg_success,
            'worker_id': worker_id,
            'completed_at': datetime.now().isoformat(),
            'status': 'completed'
        }
        
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


def store_results_table_to_iceberg(results_table: pa.Table, batch_data: Dict[str, Any]) -> bool:
    """
    Store PyArrow results table directly to Iceberg (OPTIMIZED APPROACH)
    Avoids dict->list->PyArrow conversions by working with Arrow tables throughout
    """
    if len(results_table) == 0:
        return True
    
    try:
        # Try to import iceberg_handler from current directory (automated_processing)
        try:
            import sys
            current_dir = os.path.dirname(os.path.abspath(__file__))
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            
            import iceberg_handler as ih
            
            # Initialize catalog if not already done
            if not hasattr(ih, 'iceberg_catalog') or not ih.iceberg_catalog:
                logger.info("Initializing Iceberg catalog...")
                ih.initialize_iceberg_catalog()
                
            if not ih.iceberg_catalog:
                logger.warning("Iceberg catalog not available - skipping Iceberg storage")
                return False
                
        except ImportError as e:
            logger.warning(f"iceberg_handler not available: {e} - skipping Iceberg storage")
            return False
        
        # Load existing batch_statistics table
        table = ih.iceberg_catalog.load_table("default.batch_statistics")
        
        # Prepare metadata using PyArrow compute functions (vectorized)
        current_time = datetime.now()
        event_date = current_time.strftime("%Y-%m-%d")
        num_rows = len(results_table)
        
        # Create metadata columns using simple PyArrow arrays (avoid complex compute functions)
        query_id_seq = pa.array(list(range(1, num_rows + 1)), type=pa.int64())  # 1-based indexing
        batch_id_full = f"{batch_data['session_id']}_{batch_data['batch_id']}"
        
        # Create constant arrays using simple array creation (more reliable)
        batch_ids = pa.array([batch_id_full] * num_rows, type=pa.string())
        company_names = pa.array([batch_data.get('company_name', 'unknown')] * num_rows, type=pa.string())
        event_dates = pa.array([event_date] * num_rows, type=pa.string())
        batch_numbers = pa.array([batch_data.get('batch_idx', 0)] * num_rows, type=pa.int32())
        timestamps = pa.array([current_time] * num_rows, type=pa.timestamp('us'))
        from_dialects = pa.array([batch_data.get('from_dialect', '')] * num_rows, type=pa.string())
        to_dialects = pa.array([batch_data.get('to_dialect', '')] * num_rows, type=pa.string())
        # Create empty list arrays
        empty_string_lists = pa.array([[] for _ in range(num_rows)], type=pa.list_(pa.string()))
        
        # Combine results table with metadata (vectorized column operations)
        iceberg_table = pa.table({
            "query_id": query_id_seq,
            "batch_id": batch_ids,
            "company_name": company_names,
            "event_date": event_dates,
            "batch_number": batch_numbers,
            "timestamp": timestamps,
            "status": results_table['status'],
            "executable": results_table['executable'],
            "from_dialect": from_dialects,
            "to_dialect": to_dialects,
            "original_query": results_table['original_query'],
            "converted_query": results_table['converted_query'],
            "supported_functions": results_table['supported_functions'],
            "unsupported_functions": results_table['unsupported_functions'],
            "udf_list": results_table['udf_list'] if 'udf_list' in results_table.column_names else empty_string_lists,
            "tables_list": results_table['tables_list'] if 'tables_list' in results_table.column_names else empty_string_lists,
            "processing_time_ms": results_table['processing_time_ms'],
            "error_message": results_table['error_message'],
            "unsupported_functions_after_transpilation": results_table['unsupported_functions_after_transpilation'] if 'unsupported_functions_after_transpilation' in results_table.column_names else empty_string_lists,
            "joins_list": results_table['joins_list'] if 'joins_list' in results_table.column_names else empty_string_lists
        })
        
        # Append to Iceberg table (single operation)
        table.append(iceberg_table)
        
        logger.info(f"Stored {num_rows} results to Iceberg batch_statistics table using vectorized operations")
        return True
        
    except Exception as e:
        logger.error(f"Failed to store to Iceberg: {str(e)}")
        return False

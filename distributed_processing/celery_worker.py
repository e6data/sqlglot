"""
Celery worker for distributed parquet processing
Calls the existing batch_statistics_s3 API endpoint
"""
from celery import Celery, current_task
import requests
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from redis_manager import RedisJobManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
with open('config.json', 'r') as f:
    config = json.load(f)

# Create Celery app
celery_config = config['celery']
celery_app = Celery(
    'parquet_processor',
    broker=celery_config['broker_url'],
    backend=celery_config['result_backend']
)

# Apply configuration
celery_app.conf.update(celery_config)

# Initialize Redis manager
redis_manager = RedisJobManager()

# API configuration
api_config = config['api']
processing_config = config['processing']


@celery_app.task(bind=True, max_retries=api_config.get('max_retries', 3))
def process_parquet_file(
    self,
    session_id: str,
    file_path: str,
    query_column: str,
    from_sql: str,
    to_sql: str = "e6",
    feature_flags: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Celery task that calls the existing batch_statistics_s3 API
    
    Args:
        session_id: Session identifier for this batch
        file_path: S3 path to the parquet file
        query_column: Column containing SQL queries
        from_sql: Source SQL dialect
        to_sql: Target SQL dialect
        feature_flags: Optional feature flags
    
    Returns:
        Processing results from the API
    """
    task_id = self.request.id
    start_time = datetime.now()
    
    # Update job status to processing
    redis_manager.update_job_status(task_id, 'processing')
    
    try:
        # Update Celery task state
        current_task.update_state(
            state='PROCESSING',
            meta={'file': file_path, 'session': session_id}
        )
        
        # Prepare API request
        endpoint = f"{api_config['base_url']}/batch-statistics-s3"
        data = {
            's3_path': file_path,
            'query_column': query_column,
            'from_sql': from_sql,
            'to_sql': to_sql,
            'memory_threshold_mb': processing_config.get('memory_threshold_mb', 500),
            'batch_size': processing_config.get('batch_size', 50000)
        }
        
        # Add feature flags if provided
        if not feature_flags:
            feature_flags = processing_config.get('feature_flags', {})
        
        if feature_flags:
            data['feature_flags'] = json.dumps(feature_flags)
        
        logger.info(f"Processing {file_path} via API call")
        
        # CALL THE EXISTING batch_statistics_s3 API
        response = requests.post(
            endpoint,
            data=data,
            timeout=api_config.get('timeout', 7200)
        )
        response.raise_for_status()
        result = response.json()
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Extract key metrics
        summary = {
            'file': file_path,
            'processing_time': processing_time,
            'total_queries': result.get('total_rows_processed', 0),
            'successful': result.get('total_successful', 0),
            'failed': result.get('total_failed', 0),
            'success_rate': result.get('success_rate', '0%')
        }
        
        # Update job status to completed
        redis_manager.update_job_status(task_id, 'completed', summary)
        
        logger.info(
            f"Completed {file_path}: {summary['total_queries']} queries, "
            f"{summary['success_rate']} success rate in {processing_time:.2f}s"
        )
        
        return {
            'status': 'success',
            'task_id': task_id,
            'session_id': session_id,
            'summary': summary,
            'full_result': result  # Include full API response
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        
        # Retry with exponential backoff
        retry_delay = api_config.get('retry_backoff', 60) * (self.request.retries + 1)
        
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying in {retry_delay} seconds...")
            raise self.retry(exc=e, countdown=retry_delay)
        
        # Max retries reached
        redis_manager.update_job_status(task_id, 'failed', {'error': str(e)})
        raise
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        redis_manager.update_job_status(task_id, 'failed', {'error': str(e)})
        raise


@celery_app.task
def check_session_status(session_id: str) -> Dict[str, Any]:
    """Check the status of all jobs in a session"""
    return redis_manager.get_session_status(session_id)


if __name__ == '__main__':
    # Start the worker
    celery_app.worker_main([
        'worker',
        '--loglevel=info',
        '--concurrency=4',  # Number of worker processes
        '--queues=parquet_processing'
    ])
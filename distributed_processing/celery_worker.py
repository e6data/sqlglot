"""
Simple Celery Worker - One session per parquet file
Each worker processes one session (one parquet file) at a time
"""
from celery import Celery, current_task
import requests
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import json
from redis_manager import RedisManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Celery app
app = Celery(
    'parquet_worker',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

# Configure Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=7200,  # 2 hour hard limit
    task_soft_time_limit=7000,  # Soft limit warning
    worker_prefetch_multiplier=1,  # Each worker takes 1 task at a time
    task_acks_late=True,  # Task acknowledged after completion
)

# Initialize Redis manager
redis_manager = RedisManager()

@app.task(bind=True, name='process_parquet_session')
def process_parquet_session(
    self,
    session_id: str,
    file_path: str,
    query_column: str,
    from_sql: str = 'snowflake',
    to_sql: str = 'e6',
    feature_flags: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Process a single parquet file session
    
    Each session represents ONE parquet file
    Each worker processes ONE session at a time
    """
    worker_id = f"{self.request.hostname}:{self.request.id[:8]}"
    task_id = self.request.id
    
    logger.info(f"Worker {worker_id} starting session {session_id}")
    logger.info(f"Processing file: {file_path}")
    
    # Assign this worker to the session
    redis_manager.assign_worker_to_session(session_id, worker_id, task_id)
    
    # Update Celery task state
    current_task.update_state(
        state='PROCESSING',
        meta={
            'session_id': session_id,
            'file': file_path,
            'worker': worker_id,
            'started_at': datetime.now().isoformat()
        }
    )
    
    try:
        # Call the batch_statistics_s3 API
        logger.info(f"Calling API for {file_path}")
        
        api_url = "http://localhost:8080/batch-statistics-s3"
        payload = {
            's3_path': file_path,
            'query_column': query_column,
            'from_sql': from_sql,
            'to_sql': to_sql,
            'memory_threshold_mb': 500,
            'batch_size': 50000
        }
        
        # Add feature flags if provided
        if feature_flags:
            payload['feature_flags'] = json.dumps(feature_flags)
        
        start_time = datetime.now()
        
        # Make API call
        response = requests.post(api_url, data=payload, timeout=7200)
        response.raise_for_status()
        
        result = response.json()
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Prepare success result
        session_result = {
            'status': 'success',
            'session_id': session_id,
            'file_path': file_path,
            'worker_id': worker_id,
            'processing_time': processing_time,
            'total_queries': result.get('total_rows_processed', 0),
            'successful_queries': result.get('total_successful', 0),
            'failed_queries': result.get('total_failed', 0),
            'success_rate': result.get('success_rate', '0%'),
            'api_response': result
        }
        
        # Update session status to completed
        redis_manager.update_session_status(session_id, 'completed', session_result)
        
        logger.info(
            f"Session {session_id} completed successfully. "
            f"Processed {session_result['total_queries']} queries in {processing_time:.2f}s"
        )
        
        return session_result
        
    except requests.exceptions.RequestException as e:
        error_msg = f"API request failed: {str(e)}"
        logger.error(f"Session {session_id} failed: {error_msg}")
        
        # Update session status to failed
        redis_manager.update_session_status(session_id, 'failed', {
            'error': error_msg,
            'worker_id': worker_id,
            'file_path': file_path
        })
        
        # Raise for Celery to handle retries
        raise
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Session {session_id} failed: {error_msg}")
        
        # Update session status to failed
        redis_manager.update_session_status(session_id, 'failed', {
            'error': error_msg,
            'worker_id': worker_id,
            'file_path': file_path
        })
        
        raise

@app.task(name='get_session_status')
def get_session_status(session_id: str) -> Dict[str, Any]:
    """Get status of a specific session"""
    return redis_manager.get_session(session_id)

@app.task(name='get_all_status')
def get_all_status() -> Dict[str, Any]:
    """Get status of all sessions"""
    return redis_manager.get_all_sessions_status()

if __name__ == '__main__':
    # Start worker with 4 concurrent processes
    app.worker_main([
        'worker',
        '--loglevel=info',
        '--concurrency=4',  # 4 workers, each handles 1 session at a time
        '--hostname=worker@%h',
        '-Q', 'parquet_queue'  # Queue name
    ])
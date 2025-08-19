"""
Celery Worker for Modulo Batch Processing
Implements the process_modulo_batch task for hash-based batch distribution
"""
from PIL.features import features
from celery import Celery, current_task
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Celery app
app = Celery(
    'batch_processor',
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
    task_time_limit=3600,  # 1 hour hard limit per batch
    task_soft_time_limit=3300,  # 55 minute soft limit
    worker_prefetch_multiplier=1,  # Each worker takes 1 task at a time
    task_acks_late=True,  # Task acknowledged after completion
    task_routes={
        'process_modulo_batch': {'queue': 'modulo_queue'},
    }
)


@app.task(bind=True, name='process_modulo_batch', max_retries=3)
def process_modulo_batch(
    self,
    task_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process queries matching modulo condition
    
    Args:
        task_data: Dict containing:
            - session_id: Session identifier
            - file_path: Path to parquet file
            - remainder: Modulo remainder (0 to total_batches-1)
            - total_batches: Total number of batches
            - query_column: Column containing queries
            - from_dialect: Source SQL dialect
            - to_dialect: Target SQL dialect
            - estimated_unique_per_batch: Estimated queries per batch
    
    Returns:
        Processing results
    """
    # Extract task parameters
    session_id = task_data['session_id']
    file_path = task_data['file_path']
    remainder = task_data['remainder']
    total_batches = task_data['total_batches']
    query_column = task_data['query_column']
    query_hash = task_data['query_hash']
    from_dialect = task_data['from_dialect']
    to_dialect = task_data['to_dialect']
    feature_flags = task_data['feature_flags']
    
    worker_id = f"{self.request.hostname}:{self.request.id[:8]}"
    task_id = self.request.id
    
    logger.info(f"Worker {worker_id} processing modulo batch:")
    logger.info(f"  Session: {session_id}")
    logger.info(f"  File: {file_path.split('/')[-1]}")
    logger.info(f"  Modulo: {remainder} % {total_batches}")
    
    # Import modules
    from session_manager import BatchSessionManager
    from batch_processor import process_modulo_batch_complete
    
    # Initialize session manager
    session_manager = BatchSessionManager()
    
    # Create task metadata
    task_meta_id = session_manager.create_task_metadata(
        session_id, file_path, remainder, total_batches,
        task_data.get('estimated_unique_per_batch', 0)
    )
    
    # Update task status to processing
    session_manager.update_task_status(task_meta_id, 'processing', worker_id)
    
    # Update Celery task state
    current_task.update_state(
        state='PROCESSING',
        meta={
            'session_id': session_id,
            'file': file_path.split('/')[-1],
            'worker': worker_id,
            'remainder': remainder,
            'total_batches': total_batches,
            'started_at': datetime.now().isoformat()
        }
    )
    
    try:
        # Process the modulo batch
        result = process_modulo_batch_complete(
            file_path=file_path,
            query_column=query_column,
            query_hash=query_hash,
            remainder=remainder,
            total_batches=total_batches,
            from_dialect=from_dialect,
            to_dialect=to_dialect,
            session_id=session_id,
            task_id=task_id,
            feature_flags=feature_flags,
        )
        
        # Update task status to completed
        session_manager.update_task_status(task_meta_id, 'completed', worker_id, result)
        
        # Check if session is complete
        session_manager.mark_session_completed(session_id)
        
        logger.info(f"Modulo batch {remainder} completed successfully")
        logger.info(f"  Processed {result.get('unique_queries', 0)} unique queries")
        logger.info(f"  Success rate: {result.get('successful_queries', 0)}/{result.get('unique_queries', 0)}")
        
        return result
        
    except Exception as exc:
        error_msg = str(exc)
        logger.error(f"Modulo batch {remainder} failed: {error_msg}")
        
        # Update task status to failed
        session_manager.update_task_status(task_meta_id, 'failed', worker_id, {
            'error': error_msg,
            'remainder': remainder,
            'total_batches': total_batches
        })
        
        # Retry logic
        if self.request.retries < self.max_retries:
            retry_delay = 60 * (2 ** self.request.retries)  # Exponential backoff
            logger.info(f"Retrying modulo batch {remainder} in {retry_delay}s (attempt {self.request.retries + 1})")
            raise self.retry(exc=exc, countdown=retry_delay)
        else:
            logger.error(f"Modulo batch {remainder} failed after {self.max_retries} retries")
            raise


@app.task(name='get_batch_session_status')
def get_batch_session_status(session_id: str) -> Dict[str, Any]:
    """Get status of a batch processing session"""
    from session_manager import BatchSessionManager
    session_manager = BatchSessionManager()
    return session_manager.get_session_status(session_id)


@app.task(name='get_task_status')
def get_task_status(task_id: str) -> Dict[str, Any]:
    """Get status of a specific task"""
    from session_manager import BatchSessionManager
    session_manager = BatchSessionManager()
    return session_manager.get_task_details(task_id)


if __name__ == '__main__':
    # Start worker
    app.worker_main([
        'worker',
        '--loglevel=info',
        '--concurrency=4',  # 4 workers for modulo batches
        '--hostname=batch_worker@%h',
        '-Q', 'modulo_queue'  # Process modulo queue
    ])
"""
Session Manager Module
Manages batch processing sessions with task metadata tracking
"""
import redis
import json
import uuid
import time
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class BatchSessionManager:
    """Manages batch processing sessions and task metadata"""
    
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
        self.client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.client.ping()  # Test connection
    
    def create_batch_session(
        self, 
        directory_path: str,
        from_dialect: str,
        to_dialect: str,
        total_files: int,
        total_queries: int,
        unique_queries: int,
        total_batches: int,
        file_stats: List[Dict]
    ) -> str:
        """Create a new batch processing session"""
        session_id = f"session_{uuid.uuid4().hex[:8]}"
        
        session_data = {
            'session_id': session_id,
            'directory_path': directory_path,
            'from_dialect': from_dialect,
            'to_dialect': to_dialect,
            'total_files': total_files,
            'total_queries': total_queries,
            'unique_queries': unique_queries,
            'total_batches': total_batches,
            'status': 'processing',
            'created_at': datetime.now().isoformat(),
            'file_stats': json.dumps(file_stats)
        }
        
        # Store session
        self.client.hset(f"batch_session:{session_id}", mapping=session_data)
        
        # Initialize counters
        self.client.hset(f"batch_session:{session_id}:progress", mapping={
            'completed_batches': 0,
            'failed_batches': 0,
            'processing_batches': 0,
            'pending_batches': total_batches
        })
        
        return session_id
    
    def create_task_metadata(
        self,
        session_id: str,
        file_path: str,
        remainder: int,
        total_batches: int,
        estimated_unique_per_batch: int
    ) -> str:
        """Create metadata for a modulo task"""
        task_id = f"task_{uuid.uuid4().hex[:8]}"
        
        task_meta = {
            'task_id': task_id,
            'session_id': session_id,
            'file_path': file_path,
            'remainder': remainder,
            'total_batches': total_batches,
            'estimated_unique_per_batch': estimated_unique_per_batch,
            'status': 'pending',
            'created_at': datetime.now().isoformat(),
            'worker_id': '',
            'retry_count': 0
        }
        
        # Store task metadata
        self.client.hset(f"task:{task_id}:meta", mapping=task_meta)
        
        # Add to session's task list
        self.client.sadd(f"batch_session:{session_id}:tasks", task_id)
        self.client.sadd(f"batch_session:{session_id}:pending", task_id)
        
        return task_id
    
    def update_task_status(
        self, 
        task_id: str, 
        status: str, 
        worker_id: str = None,
        result: Optional[Dict] = None
    ):
        """Update task status and session progress"""
        session_id = self.client.hget(f"task:{task_id}:meta", 'session_id')
        
        if not session_id:
            logger.error(f"Session not found for task {task_id}")
            return
        
        # Update task metadata
        updates = {
            'status': status,
            'updated_at': datetime.now().isoformat()
        }
        
        if worker_id:
            updates['worker_id'] = worker_id
        
        if status in ['completed', 'failed']:
            updates['finished_at'] = datetime.now().isoformat()
        
        if result:
            updates['result'] = json.dumps(result)
        
        self.client.hset(f"task:{task_id}:meta", mapping=updates)
        
        # Update session progress
        if status == 'processing':
            self.client.srem(f"batch_session:{session_id}:pending", task_id)
            self.client.sadd(f"batch_session:{session_id}:processing", task_id)
            self.client.hincrby(f"batch_session:{session_id}:progress", 'pending_batches', -1)
            self.client.hincrby(f"batch_session:{session_id}:progress", 'processing_batches', 1)
            
        elif status == 'completed':
            self.client.srem(f"batch_session:{session_id}:processing", task_id)
            self.client.sadd(f"batch_session:{session_id}:completed", task_id)
            self.client.hincrby(f"batch_session:{session_id}:progress", 'processing_batches', -1)
            self.client.hincrby(f"batch_session:{session_id}:progress", 'completed_batches', 1)
            
        elif status == 'failed':
            self.client.srem(f"batch_session:{session_id}:processing", task_id)
            self.client.sadd(f"batch_session:{session_id}:failed", task_id)
            self.client.hincrby(f"batch_session:{session_id}:progress", 'processing_batches', -1)
            self.client.hincrby(f"batch_session:{session_id}:progress", 'failed_batches', 1)
    
    def get_session_status(self, session_id: str) -> Dict[str, Any]:
        """Get complete session status"""
        session = self.client.hgetall(f"batch_session:{session_id}")
        
        if not session:
            return {"error": "Session not found"}
        
        progress = self.client.hgetall(f"batch_session:{session_id}:progress")
        
        # Parse file stats
        file_stats = []
        if session.get('file_stats'):
            try:
                file_stats = json.loads(session['file_stats'])
            except:
                pass
        
        # Calculate completion percentage
        total_batches = int(session.get('total_batches', 0))
        completed = int(progress.get('completed_batches', 0))
        failed = int(progress.get('failed_batches', 0))
        processing = int(progress.get('processing_batches', 0))
        pending = int(progress.get('pending_batches', 0))
        
        completion_percentage = ((completed + failed) / total_batches * 100) if total_batches > 0 else 0
        
        # Calculate performance metrics
        start_time = session.get('created_at')
        elapsed_seconds = 0
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time)
                elapsed_seconds = (datetime.now() - start_dt).total_seconds()
            except:
                pass
        
        # Estimate remaining time
        estimated_remaining_seconds = 0
        if completed > 0 and pending > 0:
            avg_time_per_batch = elapsed_seconds / completed
            estimated_remaining_seconds = avg_time_per_batch * pending
        
        return {
            'session_id': session_id,
            'directory_path': session.get('directory_path'),
            'from_dialect': session.get('from_dialect'),
            'to_dialect': session.get('to_dialect'),
            'status': session.get('status'),
            'created_at': session.get('created_at'),
            'total_files': int(session.get('total_files', 0)),
            'total_queries': int(session.get('total_queries', 0)),
            'unique_queries': int(session.get('unique_queries', 0)),
            'file_stats': file_stats,
            'progress': {
                'total_batches': total_batches,
                'completed': completed,
                'failed': failed,
                'processing': processing,
                'pending': pending,
                'completion_percentage': round(completion_percentage, 1)
            },
            'performance': {
                'elapsed_seconds': round(elapsed_seconds, 1),
                'estimated_remaining_seconds': round(estimated_remaining_seconds, 1),
                'batches_per_second': completed / elapsed_seconds if elapsed_seconds > 0 else 0
            }
        }
    
    def get_task_details(self, task_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific task"""
        task_meta = self.client.hgetall(f"task:{task_id}:meta")
        
        if not task_meta:
            return {"error": "Task not found"}
        
        # Parse result if available
        result = {}
        if task_meta.get('result'):
            try:
                result = json.loads(task_meta['result'])
            except:
                pass
        
        return {
            'task_id': task_id,
            'session_id': task_meta.get('session_id'),
            'file_path': task_meta.get('file_path'),
            'remainder': int(task_meta.get('remainder', 0)),
            'total_batches': int(task_meta.get('total_batches', 0)),
            'status': task_meta.get('status'),
            'worker_id': task_meta.get('worker_id'),
            'created_at': task_meta.get('created_at'),
            'updated_at': task_meta.get('updated_at'),
            'finished_at': task_meta.get('finished_at'),
            'retry_count': int(task_meta.get('retry_count', 0)),
            'result': result
        }
    
    def get_session_tasks(self, session_id: str) -> List[Dict[str, Any]]:
        """Get all tasks for a session"""
        task_ids = self.client.smembers(f"batch_session:{session_id}:tasks")
        
        tasks = []
        for task_id in task_ids:
            task_details = self.get_task_details(task_id)
            tasks.append(task_details)
        
        # Sort by remainder for consistent ordering
        tasks.sort(key=lambda x: x.get('remainder', 0))
        
        return tasks
    
    def mark_session_completed(self, session_id: str):
        """Mark session as completed when all tasks are done"""
        progress = self.client.hgetall(f"batch_session:{session_id}:progress")
        
        pending = int(progress.get('pending_batches', 0))
        processing = int(progress.get('processing_batches', 0))
        
        if pending == 0 and processing == 0:
            self.client.hset(f"batch_session:{session_id}", mapping={
                'status': 'completed',
                'finished_at': datetime.now().isoformat()
            })
            logger.info(f"Session {session_id} marked as completed")
            return True
        
        return False
    
    def cleanup_old_sessions(self, hours: int = 24):
        """Clean up old sessions and tasks"""
        cutoff_time = time.time() - (hours * 3600)
        
        # Find old sessions
        for key in self.client.scan_iter("batch_session:*"):
            if ":progress" in key or ":tasks" in key or ":pending" in key or ":processing" in key or ":completed" in key or ":failed" in key:
                continue
                
            session = self.client.hgetall(key)
            if session.get('created_at'):
                try:
                    created_time = datetime.fromisoformat(session['created_at']).timestamp()
                    if created_time < cutoff_time:
                        session_id = session['session_id']
                        # Delete session and related keys
                        self.client.delete(key)
                        self.client.delete(f"batch_session:{session_id}:progress")
                        self.client.delete(f"batch_session:{session_id}:tasks")
                        self.client.delete(f"batch_session:{session_id}:pending")
                        self.client.delete(f"batch_session:{session_id}:processing")
                        self.client.delete(f"batch_session:{session_id}:completed")
                        self.client.delete(f"batch_session:{session_id}:failed")
                        logger.info(f"Cleaned up old session: {session_id}")
                except:
                    pass
        
        # Clean up old tasks
        for key in self.client.scan_iter("task:*:meta"):
            task_meta = self.client.hgetall(key)
            if task_meta.get('created_at'):
                try:
                    created_time = datetime.fromisoformat(task_meta['created_at']).timestamp()
                    if created_time < cutoff_time:
                        self.client.delete(key)
                except:
                    pass
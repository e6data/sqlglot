"""
Minimal Redis Manager for Celery job management
"""
import redis
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


class RedisJobManager:
    """Minimal Redis manager for Celery job tracking"""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize Redis connection"""
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        redis_config = config['redis']
        self.client = redis.Redis(
            host=redis_config['host'],
            port=redis_config['port'],
            db=redis_config['db'],
            password=redis_config.get('password'),
            decode_responses=True
        )
        
        # Test connection
        self.client.ping()
        logger.info(f"Connected to Redis at {redis_config['host']}:{redis_config['port']}")
    
    def create_session(self, files: List[str], query_column: str) -> str:
        """Create a new job session"""
        session_id = str(uuid.uuid4())
        
        # Store basic session info
        self.client.hset(f"session:{session_id}", mapping={
            'total_files': len(files),
            'query_column': query_column,
            'created_at': datetime.now().isoformat(),
            'status': 'created'
        })
        
        # Store file list
        if files:
            self.client.rpush(f"session:{session_id}:files", *files)
        
        return session_id
    
    def add_job(self, session_id: str, task_id: str, file_path: str) -> None:
        """Add a job to session"""
        self.client.hset(f"job:{task_id}", mapping={
            'session_id': session_id,
            'file_path': file_path,
            'status': 'pending',
            'created_at': datetime.now().isoformat()
        })
        
        # Add to session's job list
        self.client.sadd(f"session:{session_id}:jobs", task_id)
    
    def update_job_status(self, task_id: str, status: str, result: Optional[Dict] = None) -> None:
        """Update job status"""
        updates = {
            'status': status,
            'updated_at': datetime.now().isoformat()
        }
        
        if result:
            updates['result'] = json.dumps(result)
        
        self.client.hset(f"job:{task_id}", mapping=updates)
    
    def get_session_status(self, session_id: str) -> Dict[str, Any]:
        """Get session status with all jobs"""
        session_info = self.client.hgetall(f"session:{session_id}")
        if not session_info:
            return {'error': 'Session not found'}
        
        # Get all job IDs
        job_ids = self.client.smembers(f"session:{session_id}:jobs")
        
        # Get job statuses
        jobs = []
        for job_id in job_ids:
            job_data = self.client.hgetall(f"job:{job_id}")
            if job_data:
                if 'result' in job_data:
                    job_data['result'] = json.loads(job_data['result'])
                jobs.append(job_data)
        
        # Calculate summary
        total = len(jobs)
        completed = sum(1 for j in jobs if j.get('status') == 'completed')
        failed = sum(1 for j in jobs if j.get('status') == 'failed')
        pending = sum(1 for j in jobs if j.get('status') == 'pending')
        processing = sum(1 for j in jobs if j.get('status') == 'processing')
        
        return {
            'session_id': session_id,
            'session_info': session_info,
            'summary': {
                'total': total,
                'completed': completed,
                'failed': failed,
                'pending': pending,
                'processing': processing,
                'progress': (completed + failed) / total * 100 if total > 0 else 0
            },
            'jobs': jobs
        }
    
    def cleanup_old_data(self, hours: int = 24) -> None:
        """Clean up old sessions and jobs"""
        # This is a simple cleanup - in production you'd want more sophisticated logic
        for key in self.client.scan_iter("session:*"):
            self.client.expire(key, hours * 3600)
        
        for key in self.client.scan_iter("job:*"):
            self.client.expire(key, hours * 3600)
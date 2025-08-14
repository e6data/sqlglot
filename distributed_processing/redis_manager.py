"""
Simple Redis Manager - One session per parquet file
"""
import redis
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional

class RedisManager:
    """Simple Redis manager for per-file session tracking"""
    
    def __init__(self):
        self.client = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True
        )
        self.client.ping()  # Test connection
    
    def create_file_session(self, file_path: str, query_column: str) -> str:
        """Create a session for a single parquet file"""
        session_id = str(uuid.uuid4())[:8]  # Shorter IDs for simplicity
        
        session_data = {
            'session_id': session_id,
            'file_path': file_path,
            'file_name': file_path.split('/')[-1],
            'query_column': query_column,
            'status': 'pending',
            'created_at': datetime.now().isoformat(),
            'worker_id': '',
            'task_id': ''
        }
        
        # Store session
        self.client.hset(f"session:{session_id}", mapping=session_data)
        
        # Add to pending queue
        self.client.sadd("sessions:pending", session_id)
        
        return session_id
    
    def assign_worker_to_session(self, session_id: str, worker_id: str, task_id: str):
        """Assign a worker to process a session"""
        # Update session
        self.client.hset(f"session:{session_id}", mapping={
            'worker_id': worker_id,
            'task_id': task_id,
            'status': 'processing',
            'started_at': datetime.now().isoformat()
        })
        
        # Move from pending to processing
        self.client.srem("sessions:pending", session_id)
        self.client.sadd("sessions:processing", session_id)
    
    def update_session_status(self, session_id: str, status: str, result: Optional[Dict] = None):
        """Update session status"""
        updates = {
            'status': status,
            'updated_at': datetime.now().isoformat()
        }
        
        if status == 'completed' or status == 'failed':
            updates['finished_at'] = datetime.now().isoformat()
            
            # Move from processing to completed/failed
            self.client.srem("sessions:processing", session_id)
            self.client.sadd(f"sessions:{status}", session_id)
        
        if result:
            updates['result'] = json.dumps(result)
        
        self.client.hset(f"session:{session_id}", mapping=updates)
    
    def get_session(self, session_id: str) -> Dict[str, Any]:
        """Get session details"""
        session = self.client.hgetall(f"session:{session_id}")
        if session and 'result' in session:
            session['result'] = json.loads(session['result'])
        return session
    
    def get_all_sessions_status(self) -> Dict[str, Any]:
        """Get status of all sessions"""
        pending = list(self.client.smembers("sessions:pending"))
        processing = list(self.client.smembers("sessions:processing"))
        completed = list(self.client.smembers("sessions:completed"))
        failed = list(self.client.smembers("sessions:failed"))
        
        return {
            'summary': {
                'total': len(pending) + len(processing) + len(completed) + len(failed),
                'pending': len(pending),
                'processing': len(processing),
                'completed': len(completed),
                'failed': len(failed)
            },
            'sessions': {
                'pending': pending,
                'processing': processing,
                'completed': completed,
                'failed': failed
            }
        }
    
    def cleanup(self, hours: int = 24):
        """Clean up old sessions"""
        for pattern in ["session:*", "sessions:*"]:
            for key in self.client.scan_iter(pattern):
                self.client.expire(key, hours * 3600)
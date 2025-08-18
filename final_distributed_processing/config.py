"""
Configuration for Final Distributed Processing System
"""

# Redis Configuration
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}

# Celery Configuration
CELERY_CONFIG = {
    "broker_url": "redis://localhost:6379/0",
    "result_backend": "redis://localhost:6379/0",
    "task_time_limit": 3600,  # 1 hour
    "task_soft_time_limit": 3300,  # 55 minutes
    "worker_prefetch_multiplier": 1,
    "task_acks_late": True
}

# Processing Configuration
PROCESSING_CONFIG = {
    "default_batch_size": 10000,
    "max_batch_size": 20000,
    "min_batch_size": 1000,
    "chunk_size": 50000,
    "default_from_dialect": "databricks",
    "default_to_dialect": "e6",
    "api_base_url": "http://localhost:8080"
}

# Queue Names
QUEUE_NAMES = {
    "modulo_queue": "modulo_queue",
    "session_queue": "session_queue"
}
"""
Orchestrator for distributed parquet processing
This script coordinates the validation and distributed processing workflow
"""
import json
import logging
import requests
from typing import List, Dict, Any, Optional
import sys
import time
from redis_manager import RedisJobManager
from celery_worker import process_parquet_file, check_session_status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
with open('config.json', 'r') as f:
    config = json.load(f)


class ParquetOrchestrator:
    """Orchestrates distributed parquet processing"""
    
    def __init__(self):
        self.redis_manager = RedisJobManager()
        self.api_base_url = config['api']['base_url']
        self.processing_config = config['processing']
    
    def validate_s3_bucket(self, s3_path: str) -> Dict[str, Any]:
        """
        Call validate-s3-bucket API to get parquet files and columns
        
        Args:
            s3_path: S3 path to validate
        
        Returns:
            Validation results including files and columns
        """
        endpoint = f"{self.api_base_url}/validate-s3-bucket"
        
        logger.info(f"Validating S3 path: {s3_path}")
        
        try:
            response = requests.post(endpoint, data={'s3_path': s3_path})
            response.raise_for_status()
            result = response.json()
            
            if result.get('authenticated'):
                logger.info(f"Found {result.get('files_found', 0)} parquet files")
                if result.get('query_column'):
                    logger.info(f"Auto-detected query column: {result['query_column']}")
                else:
                    logger.info("No query column auto-detected")
                    if result.get('common_columns'):
                        logger.info(f"Available columns: {', '.join(result['common_columns'][:5])}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Validation failed: {e}")
            return {'error': str(e)}
    
    def submit_distributed_jobs(
        self,
        parquet_files: List[str],
        query_column: str,
        from_sql: str = None,
        to_sql: str = None,
        feature_flags: Optional[Dict] = None
    ) -> str:
        """
        Submit parquet files for distributed processing
        
        Args:
            parquet_files: List of S3 paths to process
            query_column: Column containing queries
            from_sql: Source SQL dialect
            to_sql: Target SQL dialect
            feature_flags: Optional feature flags
        
        Returns:
            Session ID for tracking
        """
        # Use defaults from config if not provided
        if not from_sql:
            from_sql = self.processing_config.get('default_from_sql', 'snowflake')
        if not to_sql:
            to_sql = self.processing_config.get('default_to_sql', 'e6')
        
        # Create session
        session_id = self.redis_manager.create_session(parquet_files, query_column)
        logger.info(f"Created session: {session_id}")
        
        # Submit tasks
        task_ids = []
        for file_path in parquet_files:
            # Submit Celery task
            task = process_parquet_file.apply_async(
                args=[session_id, file_path, query_column, from_sql, to_sql, feature_flags],
                queue='parquet_processing'
            )
            
            # Track in Redis
            self.redis_manager.add_job(session_id, task.id, file_path)
            task_ids.append(task.id)
            
            logger.info(f"Submitted job {task.id} for {file_path}")
        
        logger.info(f"Submitted {len(task_ids)} jobs for processing")
        
        return session_id
    
    def monitor_session(self, session_id: str, poll_interval: int = 5) -> Dict[str, Any]:
        """
        Monitor session progress
        
        Args:
            session_id: Session to monitor
            poll_interval: Seconds between status checks
        
        Returns:
            Final session status
        """
        logger.info(f"Monitoring session {session_id}")
        
        while True:
            status = self.redis_manager.get_session_status(session_id)
            
            if 'error' in status:
                logger.error(f"Session error: {status['error']}")
                return status
            
            summary = status.get('summary', {})
            progress = summary.get('progress', 0)
            
            logger.info(
                f"Progress: {progress:.1f}% | "
                f"Completed: {summary.get('completed', 0)} | "
                f"Failed: {summary.get('failed', 0)} | "
                f"Processing: {summary.get('processing', 0)} | "
                f"Pending: {summary.get('pending', 0)}"
            )
            
            # Check if all jobs are done
            if summary.get('pending', 0) == 0 and summary.get('processing', 0) == 0:
                logger.info("All jobs completed!")
                return status
            
            time.sleep(poll_interval)
    
    def process_s3_directory(
        self,
        s3_path: str,
        query_column: Optional[str] = None,
        from_sql: str = None,
        to_sql: str = None,
        monitor: bool = True
    ) -> Dict[str, Any]:
        """
        Complete workflow: validate, submit, and optionally monitor
        
        Args:
            s3_path: S3 path to process
            query_column: Query column (auto-detected if not provided)
            from_sql: Source SQL dialect
            to_sql: Target SQL dialect
            monitor: Whether to monitor until completion
        
        Returns:
            Processing results
        """
        # Step 1: Validate S3 bucket
        validation = self.validate_s3_bucket(s3_path)
        
        if validation.get('error') or not validation.get('authenticated'):
            logger.error("Validation failed")
            return validation
        
        # Get parquet files
        # For directory processing, we'd need to modify validate_s3_bucket to return file list
        # For now, assume single file
        parquet_files = [s3_path] if s3_path.endswith('.parquet') else []
        
        if not parquet_files:
            # If directory, extract files from validation
            if 'sample_files' in validation:
                parquet_files = validation['sample_files']
            else:
                logger.error("No parquet files found")
                return {'error': 'No parquet files found'}
        
        # Step 2: Determine query column
        if not query_column:
            query_column = validation.get('query_column')
            
            if not query_column:
                if validation.get('common_columns'):
                    logger.error(f"Please specify query column from: {validation['common_columns']}")
                    return {'error': 'Query column required', 'available_columns': validation['common_columns']}
                else:
                    return {'error': 'No columns found'}
        
        # Step 3: Submit for distributed processing
        session_id = self.submit_distributed_jobs(
            parquet_files=parquet_files,
            query_column=query_column,
            from_sql=from_sql,
            to_sql=to_sql
        )
        
        result = {'session_id': session_id, 'files_submitted': len(parquet_files)}
        
        # Step 4: Monitor if requested
        if monitor:
            final_status = self.monitor_session(session_id)
            result['final_status'] = final_status
        
        return result


def main():
    """Main entry point for command-line usage"""
    if len(sys.argv) < 2:
        print("Usage: python orchestrator.py <s3_path> [query_column] [from_sql] [to_sql]")
        sys.exit(1)
    
    s3_path = sys.argv[1]
    query_column = sys.argv[2] if len(sys.argv) > 2 else None
    from_sql = sys.argv[3] if len(sys.argv) > 3 else None
    to_sql = sys.argv[4] if len(sys.argv) > 4 else None
    
    orchestrator = ParquetOrchestrator()
    
    result = orchestrator.process_s3_directory(
        s3_path=s3_path,
        query_column=query_column,
        from_sql=from_sql,
        to_sql=to_sql,
        monitor=True
    )
    
    print("\nFinal Result:")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
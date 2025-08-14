"""
Simplified Orchestrator - One session per parquet file
Coordinates the distributed processing workflow using actual API endpoints
"""
import json
import logging
import requests
import sys
import time
from typing import List, Dict, Any, Optional
from pathlib import Path
from celery_worker import app, process_parquet_session
from redis_manager import RedisManager
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load configuration
import os
config_path = os.path.join(os.path.dirname(__file__), 'config.json')
with open(config_path, 'r') as f:
    config = json.load(f)

class ParquetOrchestrator:
    """Orchestrates distributed parquet processing with one session per file"""
    
    def __init__(self):
        self.redis_manager = RedisManager()
        self.api_config = config['api']
        self.processing_config = config['processing']
        
    def validate_s3_path(self, s3_path: str) -> Dict[str, Any]:
        """
        Validate S3 path using the /validate-s3-bucket endpoint
        
        Args:
            s3_path: S3 path to validate (file or directory)
        
        Returns:
            Validation results with file list and columns
        """
        endpoint = f"{self.api_config['base_url']}/validate-s3-bucket"
        
        logger.info(f"Validating S3 path: {s3_path}")
        
        try:
            response = requests.post(
                endpoint, 
                data={'s3_path': s3_path},
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get('authenticated'):
                logger.info(f"Validation successful")
                logger.info(f"Files found: {result.get('files_found', 0)}")
                logger.info(f"Query column: {result.get('query_column', 'Not detected')}")
                logger.info(f"Total size: {result.get('total_size_mb', 0)} MB")
            else:
                logger.error(f"S3 authentication failed: {result.get('error')}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Validation API call failed: {e}")
            return {'error': str(e), 'authenticated': False}
    
    def get_parquet_files_from_s3(self, s3_path: str) -> List[str]:
        """
        Get list of parquet files from S3 path using validation endpoint
        
        Args:
            s3_path: S3 path (file or directory)
        
        Returns:
            List of S3 parquet file paths
        """
        # If it's a single parquet file, return it directly
        if s3_path.endswith('.parquet'):
            return [s3_path]
        
        # For directories, use validation endpoint to get file list
        validation = self.validate_s3_path(s3_path)
        
        if not validation.get('authenticated'):
            logger.error(f"Failed to authenticate with S3: {validation.get('error')}")
            return []
        
        # Get sample files from validation response
        sample_files = validation.get('sample_files', [])
        
        if sample_files:
            # Convert to full S3 paths if needed
            files = []
            for file_path in sample_files:
                if file_path.startswith('s3://'):
                    files.append(file_path)
                else:
                    # Construct full S3 path
                    if file_path.startswith(s3_path.split('/')[2]):
                        # File path includes bucket name
                        files.append(f"s3://{file_path}")
                    else:
                        # File path is relative to the directory
                        base_path = s3_path.rstrip('/')
                        files.append(f"{base_path}/{file_path}")
            return files
        
        return []
    
    def submit_file_for_processing(
        self,
        file_path: str,
        query_column: str,
        from_sql: str = None,
        to_sql: str = None,
        feature_flags: Optional[Dict] = None
    ) -> Dict[str, str]:
        """
        Submit a single parquet file for processing
        Each file gets its own unique session ID
        
        Args:
            file_path: S3 path to parquet file
            query_column: Column containing queries
            from_sql: Source SQL dialect
            to_sql: Target SQL dialect
            feature_flags: Optional feature flags dict
        
        Returns:
            Dict with session_id, task_id, and file_path
        """
        # Use defaults from config if not provided
        if not from_sql:
            from_sql = self.processing_config.get('default_from_sql', 'snowflake')
        if not to_sql:
            to_sql = self.processing_config.get('default_to_sql', 'e6')
        if not feature_flags:
            feature_flags = self.processing_config.get('feature_flags', {})
        
        # Create unique session for this specific file
        session_id = self.redis_manager.create_file_session(file_path, query_column)
        
        # Submit to Celery with the session ID
        task = process_parquet_session.apply_async(
            args=[session_id, file_path, query_column, from_sql, to_sql, feature_flags],
            queue='parquet_queue'
        )
        
        logger.info(f"Submitted file: {file_path.split('/')[-1]}")
        logger.info(f"  Session ID: {session_id}")
        logger.info(f"  Task ID: {task.id}")
        
        return {
            'session_id': session_id,
            'task_id': task.id,
            'file_path': file_path,
            'file_name': file_path.split('/')[-1]
        }
    
    def submit_batch(
        self,
        parquet_files: List[str],
        query_column: str,
        from_sql: str = None,
        to_sql: str = None,
        feature_flags: Optional[Dict] = None
    ) -> List[Dict[str, str]]:
        """
        Submit multiple parquet files for processing
        Each file gets its own session ID for independent tracking
        
        Args:
            parquet_files: List of S3 paths
            query_column: Column containing queries
            from_sql: Source SQL dialect
            to_sql: Target SQL dialect
            feature_flags: Optional feature flags
        
        Returns:
            List of submission results with session IDs
        """
        submissions = []
        
        logger.info(f"Submitting {len(parquet_files)} files for distributed processing")
        logger.info(f"Each file will have its own session ID")
        
        for i, file_path in enumerate(parquet_files, 1):
            result = self.submit_file_for_processing(
                file_path, query_column, from_sql, to_sql, feature_flags
            )
            submissions.append(result)
            
            # Log progress for large batches
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(parquet_files)} files submitted")
        
        logger.info(f"All {len(submissions)} files submitted with individual sessions")
        
        return submissions
    
    def monitor_sessions(self, session_ids: List[str] = None, poll_interval: int = 5) -> None:
        """
        Monitor progress of sessions
        
        Args:
            session_ids: Specific session IDs to monitor (None for all)
            poll_interval: Seconds between status checks
        """
        logger.info("Monitoring session progress...")
        
        last_status = None
        start_time = time.time()
        
        while True:
            status = self.redis_manager.get_all_sessions_status()
            summary = status['summary']
            
            # Only log if status changed
            if summary != last_status:
                elapsed = int(time.time() - start_time)
                logger.info(
                    f"[{elapsed:3d}s] "
                    f"Pending: {summary['pending']} | "
                    f"Processing: {summary['processing']} | "
                    f"Completed: {summary['completed']} | "
                    f"Failed: {summary['failed']}"
                )
                last_status = summary
            
            # Check if all done
            if summary['pending'] == 0 and summary['processing'] == 0:
                logger.info("All sessions completed!")
                
                # Show final results
                if summary['completed'] > 0:
                    logger.info(f"✓ {summary['completed']} files processed successfully")
                if summary['failed'] > 0:
                    logger.warning(f"✗ {summary['failed']} files failed")
                    
                    # Show failed sessions
                    for session_id in status['sessions']['failed']:
                        session = self.redis_manager.get_session(session_id)
                        logger.error(f"  Failed: {session.get('file_name', session_id)}")
                        if session.get('result'):
                            error = session['result'].get('error', 'Unknown error')
                            logger.error(f"    Error: {error}")
                
                break
            
            time.sleep(poll_interval)
    
    def process_s3_directory(
        self,
        s3_path: str,
        query_column: Optional[str] = None,
        from_sql: str = None,
        to_sql: str = None,
        feature_flags: Optional[Dict] = None,
        monitor: bool = True
    ) -> Dict[str, Any]:
        """
        Complete workflow for processing S3 directory or file
        
        Args:
            s3_path: S3 path to process
            query_column: Query column (auto-detect if not provided)
            from_sql: Source SQL dialect
            to_sql: Target SQL dialect
            feature_flags: Optional feature flags
            monitor: Whether to monitor until completion
        
        Returns:
            Processing results with session IDs
        """
        # Step 1: Validate and get files
        logger.info(f"Starting distributed processing for: {s3_path}")
        
        validation = self.validate_s3_path(s3_path)
        
        if not validation.get('authenticated'):
            logger.error("S3 validation failed")
            return {'error': 'S3 validation failed', 'details': validation}
        
        # Get parquet files
        parquet_files = self.get_parquet_files_from_s3(s3_path)
        
        if not parquet_files:
            # Try to get from validation response
            if 'sample_files' in validation and validation['sample_files']:
                parquet_files = []
                for file in validation['sample_files']:
                    if file.startswith('s3://'):
                        parquet_files.append(file)
                    else:
                        # Construct full path
                        bucket = s3_path.split('/')[2]
                        parquet_files.append(f"s3://{file}")
            
            if not parquet_files:
                logger.error("No parquet files found")
                return {'error': 'No parquet files found', 'path': s3_path}
        
        logger.info(f"Found {len(parquet_files)} parquet files to process")
        
        # Step 2: Determine query column if not provided
        if not query_column:
            query_column = validation.get('query_column')
            
            if not query_column:
                # Try common column names
                common_names = ['query_string', 'query', 'sql_query', 'sql', 'hashed_query']
                available_columns = validation.get('common_columns', [])
                
                for col_name in common_names:
                    if col_name in available_columns:
                        query_column = col_name
                        logger.info(f"Auto-selected query column: {query_column}")
                        break
                
                if not query_column:
                    logger.error("Could not determine query column")
                    return {
                        'error': 'Query column required',
                        'available_columns': available_columns,
                        'suggestion': 'Please specify query_column parameter'
                    }
        
        logger.info(f"Using query column: {query_column}")
        
        # Step 3: Submit all files with individual sessions
        submissions = self.submit_batch(
            parquet_files, query_column, from_sql, to_sql, feature_flags
        )
        
        result = {
            'total_files': len(submissions),
            'submissions': submissions,
            'query_column': query_column,
            'from_sql': from_sql or self.processing_config.get('default_from_sql'),
            'to_sql': to_sql or self.processing_config.get('default_to_sql')
        }
        
        # Step 4: Monitor if requested
        if monitor:
            self.monitor_sessions()
            
            # Get final status
            final_status = self.redis_manager.get_all_sessions_status()
            result['final_summary'] = final_status['summary']
            
            # Add detailed results for each session
            result['session_results'] = []
            for submission in submissions:
                session = self.redis_manager.get_session(submission['session_id'])
                result['session_results'].append({
                    'file': submission['file_name'],
                    'session_id': submission['session_id'],
                    'status': session.get('status'),
                    'worker_id': session.get('worker_id'),
                    'processing_time': session.get('result', {}).get('processing_time') if session.get('result') else None,
                    'total_queries': session.get('result', {}).get('total_queries') if session.get('result') else None,
                    'success_rate': session.get('result', {}).get('success_rate') if session.get('result') else None
                })
        
        return result
    
    def get_session_details(self, session_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific session"""
        return self.redis_manager.get_session(session_id)
    
    def get_all_status(self) -> Dict[str, Any]:
        """Get status of all sessions"""
        return self.redis_manager.get_all_sessions_status()


def main():
    """Main entry point for command-line usage"""
    if len(sys.argv) < 2:
        print("Usage: python orchestrator.py <s3_path> [query_column] [from_sql] [to_sql]")
        print("\nExamples:")
        print("  python orchestrator.py s3://bucket/file.parquet")
        print("  python orchestrator.py s3://bucket/directory/ query_string")
        print("  python orchestrator.py s3://bucket/directory/ query_string snowflake e6")
        sys.exit(1)
    
    s3_path = sys.argv[1]
    query_column = sys.argv[2] if len(sys.argv) > 2 else None
    from_sql = sys.argv[3] if len(sys.argv) > 3 else None
    to_sql = sys.argv[4] if len(sys.argv) > 4 else None
    
    orchestrator = ParquetOrchestrator()
    
    # Process the directory/file
    result = orchestrator.process_s3_directory(
        s3_path=s3_path,
        query_column=query_column,
        from_sql=from_sql,
        to_sql=to_sql,
        monitor=True
    )
    
    # Print summary
    print("\n" + "="*60)
    print("DISTRIBUTED PROCESSING COMPLETE")
    print("="*60)
    
    if 'error' in result:
        print(f"Error: {result['error']}")
        if 'details' in result:
            print(f"Details: {result.get('details')}")
    else:
        summary = result.get('final_summary', {})
        print(f"Total Files Submitted: {result['total_files']}")
        print(f"Successfully Completed: {summary.get('completed', 0)}")
        print(f"Failed: {summary.get('failed', 0)}")
        print(f"Query Column: {result['query_column']}")
        print(f"Transpilation: {result['from_sql']} → {result['to_sql']}")
        
        # Show session details
        if 'session_results' in result:
            print("\n" + "-"*60)
            print("Session Results:")
            print("-"*60)
            for session_result in result['session_results'][:10]:  # Show first 10
                print(f"\nFile: {session_result['file']}")
                print(f"  Session ID: {session_result['session_id']}")
                print(f"  Status: {session_result['status']}")
                if session_result['status'] == 'completed':
                    print(f"  Total Queries: {session_result.get('total_queries', 'N/A')}")
                    print(f"  Success Rate: {session_result.get('success_rate', 'N/A')}")
                    print(f"  Processing Time: {session_result.get('processing_time', 0):.2f}s")
            
            if len(result['session_results']) > 10:
                print(f"\n... and {len(result['session_results']) - 10} more files")
        
        print("\n" + "="*60)
        print("Use Redis to query individual session details:")
        print(f"  python -c \"from redis_manager import RedisManager; import json;")
        print(f"  m = RedisManager(); print(json.dumps(m.get_session('<session_id>'), indent=2))\"")
        print("="*60)


if __name__ == "__main__":
    main()
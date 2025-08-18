"""
Test Script for Final Distributed Processing System
Tests the complete hash-based modulo batch processing workflow
"""
import requests
import time
import json
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_BASE_URL = "http://localhost:8080"

def test_process_parquet_directory():
    """Test the /process-parquet-directory endpoint"""
    logger.info("Testing /process-parquet-directory endpoint")
    
    # Use test directory
    test_directory = "../distributed_processing/test"
    
    payload = {
        "directory_path": test_directory,
        "from_dialect": "databricks", 
        "to_dialect": "e6",
        "batch_size": 1000,  # Small batch size for testing
        "use_batch_processing": True,
        "query_column": "hashed_query"
    }
    
    try:
        logger.info(f"Submitting directory: {test_directory}")
        response = requests.post(f"{API_BASE_URL}/process-parquet-directory", data=payload, timeout=60)
        
        if response.status_code == 200:
            result = response.json()
            logger.info("Directory processing started successfully!")
            logger.info(f"Session ID: {result['session_id']}")
            logger.info(f"Total files: {result['total_files']}")
            logger.info(f"Total queries: {result['total_queries']}")
            logger.info(f"Unique queries: {result['unique_queries']}")
            logger.info(f"Total batches: {result['total_batches']}")
            logger.info(f"Estimated time: {result['estimated_time_minutes']} minutes")
            
            return result['session_id']
        else:
            logger.error(f"API call failed: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error testing directory processing: {str(e)}")
        return None

def monitor_session_progress(session_id: str, poll_interval: int = 5):
    """Monitor session progress until completion"""
    logger.info(f"Monitoring session: {session_id}")
    
    start_time = time.time()
    last_status = None
    
    while True:
        try:
            response = requests.get(f"{API_BASE_URL}/status/{session_id}", timeout=30)
            
            if response.status_code == 200:
                status = response.json()
                
                if status.get("error"):
                    logger.error(f"Session error: {status['error']}")
                    break
                
                progress = status.get('progress', {})
                
                # Only log if status changed
                current_summary = (
                    progress.get('completed', 0),
                    progress.get('failed', 0), 
                    progress.get('processing', 0),
                    progress.get('pending', 0)
                )
                
                if current_summary != last_status:
                    elapsed = int(time.time() - start_time)
                    logger.info(
                        f"[{elapsed:3d}s] "
                        f"Completed: {progress.get('completed', 0)} | "
                        f"Failed: {progress.get('failed', 0)} | "
                        f"Processing: {progress.get('processing', 0)} | "
                        f"Pending: {progress.get('pending', 0)} | "
                        f"Progress: {progress.get('completion_percentage', 0):.1f}%"
                    )
                    last_status = current_summary
                
                # Check if completed
                if progress.get('pending', 0) == 0 and progress.get('processing', 0) == 0:
                    logger.info("Session completed!")
                    
                    performance = status.get('performance', {})
                    logger.info(f"Final results:")
                    logger.info(f"  Total batches: {progress.get('total_batches', 0)}")
                    logger.info(f"  Completed: {progress.get('completed', 0)}")
                    logger.info(f"  Failed: {progress.get('failed', 0)}")
                    logger.info(f"  Total time: {performance.get('elapsed_seconds', 0):.1f}s")
                    logger.info(f"  Batches/second: {performance.get('batches_per_second', 0):.2f}")
                    
                    return status
                    
            else:
                logger.error(f"Status check failed: {response.status_code}")
                break
                
        except Exception as e:
            logger.error(f"Error checking status: {str(e)}")
            break
        
        time.sleep(poll_interval)
    
    return None

def test_individual_endpoints():
    """Test individual API endpoints"""
    logger.info("Testing individual endpoints...")
    
    # Test health check
    try:
        response = requests.get(f"{API_BASE_URL}/health")
        logger.info(f"Health check: {response.status_code}")
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
    
    # Test iceberg stats
    try:
        response = requests.get(f"{API_BASE_URL}/iceberg-stats")
        if response.status_code == 200:
            stats = response.json()
            logger.info(f"Iceberg stats: {stats.get('table_stats', {})}")
        else:
            logger.error(f"Iceberg stats failed: {response.status_code}")
    except Exception as e:
        logger.error(f"Iceberg stats error: {str(e)}")

def main():
    """Main test function"""
    logger.info("="*60)
    logger.info("TESTING FINAL DISTRIBUTED PROCESSING SYSTEM")
    logger.info("="*60)
    
    # Test individual endpoints first
    test_individual_endpoints()
    
    # Test directory processing
    session_id = test_process_parquet_directory()
    
    if session_id:
        logger.info(f"\nMonitoring session {session_id}...")
        final_status = monitor_session_progress(session_id)
        
        if final_status:
            logger.info("\n" + "="*60)
            logger.info("TEST COMPLETED SUCCESSFULLY")
            logger.info("="*60)
        else:
            logger.error("\n" + "="*60)
            logger.error("TEST FAILED OR INCOMPLETE")
            logger.error("="*60)
    else:
        logger.error("Failed to start directory processing")

if __name__ == "__main__":
    main()
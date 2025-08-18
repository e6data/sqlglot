"""
API Endpoints for Distributed Batch Processing
Contains the /process-parquet-directory endpoint and related functions
"""
import json
import logging
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from fastapi import Form, HTTPException
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_parquet_directory_handler(
    directory_path: str,
    from_dialect: str,
    to_dialect: str = "e6",
    batch_size: int = 10000,
    use_batch_processing: bool = True,
    query_column: str = "hashed_query"
) -> Dict[str, Any]:
    """
    Handler for /process-parquet-directory endpoint
    Implements hash-based modulo distribution as per architecture
    """
    timestamp = datetime.now().isoformat()
    session_id = f"session_{uuid.uuid4().hex[:8]}"
    
    logger.info(f"Starting directory processing - Session: {session_id}")
    logger.info(f"Directory: {directory_path}")
    logger.info(f"Transpilation: {from_dialect} -> {to_dialect}")
    
    try:
        # Import modules
        from batch_processor import calculate_optimal_batches
        from session_manager import BatchSessionManager
        
        # Initialize session manager
        session_manager = BatchSessionManager()
        
        # Get parquet files from directory
        directory_path_obj = Path(directory_path)
        if not directory_path_obj.exists():
            raise HTTPException(status_code=404, detail=f"Directory not found: {directory_path}")
        
        parquet_files = []
        if directory_path_obj.is_file() and directory_path_obj.suffix == '.parquet':
            parquet_files = [str(directory_path_obj.absolute())]
        elif directory_path_obj.is_dir():
            parquet_files = [str(f.absolute()) for f in directory_path_obj.glob('*.parquet')]
        
        if not parquet_files:
            raise HTTPException(status_code=404, detail=f"No parquet files found in: {directory_path}")
        
        logger.info(f"Found {len(parquet_files)} parquet files")
        
        # Pre-scan files to determine total unique queries and optimal batching
        total_queries = 0
        total_unique_queries = 0
        total_batches = 0
        pre_scan_start = time.time()
        
        file_stats = []
        
        for file_path in parquet_files:
            try:
                # Use the batch_processor pre-scan function
                scan_result = calculate_optimal_batches(file_path, query_column, batch_size)
                
                if scan_result.get("error"):
                    logger.error(f"Error scanning {Path(file_path).name}: {scan_result['error']}")
                    continue
                
                file_stats.append({
                    "file_path": file_path,
                    "file_name": scan_result["file_name"],
                    "total_rows": scan_result["total_count"],
                    "unique_queries": scan_result["unique_count"],
                    "batches": scan_result["num_batches"],
                    "queries_per_batch": scan_result["queries_per_batch"],
                    "scan_time_seconds": scan_result["scan_time_seconds"]
                })
                
                total_queries += scan_result["total_count"]
                total_unique_queries += scan_result["unique_count"]
                total_batches += scan_result["num_batches"]
                
                logger.info(f"File: {scan_result['file_name']} - {scan_result['total_count']} rows, {scan_result['unique_count']} unique, {scan_result['num_batches']} batches")
                
            except Exception as e:
                logger.error(f"Error pre-scanning {file_path}: {str(e)}")
                continue
        
        pre_scan_time = time.time() - pre_scan_start
        
        # Estimate processing time (based on ~500 queries/second)
        estimated_time_minutes = total_unique_queries / (500 * 60)
        
        logger.info(f"Pre-scan completed in {pre_scan_time:.2f}s")
        logger.info(f"Total: {total_queries} queries, {total_unique_queries} unique")
        logger.info(f"Will create {total_batches} total batches")
        logger.info(f"Estimated processing time: {estimated_time_minutes:.1f} minutes")
        
        # Create batch session
        session_id = session_manager.create_batch_session(
            directory_path=directory_path,
            from_dialect=from_dialect,
            to_dialect=to_dialect,
            total_files=len(file_stats),
            total_queries=total_queries,
            unique_queries=total_unique_queries,
            total_batches=total_batches,
            file_stats=file_stats
        )
        
        # Submit modulo tasks to Celery
        submitted_tasks = []
        
        try:
            from celery_worker import app as celery_app
            
            for file_stat in file_stats:
                file_path = file_stat["file_path"]
                num_batches = file_stat["batches"]
                
                # Submit modulo tasks for this file
                for remainder in range(num_batches):
                    task_data = {
                        "session_id": session_id,
                        "file_path": file_path,
                        "remainder": remainder,
                        "total_batches": num_batches,
                        "query_column": query_column,
                        "from_dialect": from_dialect,
                        "to_dialect": to_dialect,
                        "estimated_unique_per_batch": file_stat["queries_per_batch"]
                    }
                    
                    # Submit to modulo queue
                    task = celery_app.send_task(
                        "process_modulo_batch",
                        args=[task_data],
                        queue='modulo_queue'
                    )
                    
                    submitted_tasks.append({
                        "task_id": task.id,
                        "file_name": file_stat["file_name"],
                        "remainder": remainder,
                        "total_batches": num_batches
                    })
            
            logger.info(f"Submitted {len(submitted_tasks)} modulo tasks to Celery")
            
        except Exception as celery_error:
            logger.error(f"Failed to submit Celery tasks: {celery_error}")
            raise HTTPException(status_code=500, detail=f"Failed to submit tasks: {str(celery_error)}")
        
        return {
            "session_id": session_id,
            "total_files": len(file_stats),
            "total_queries": total_queries,
            "unique_queries": total_unique_queries,
            "pre_scan_time_seconds": round(pre_scan_time, 2),
            "total_batches": total_batches,
            "estimated_time_minutes": round(estimated_time_minutes, 1),
            "status": "processing",
            "tracking_url": f"/status/{session_id}",
            "submitted_tasks": len(submitted_tasks),
            "file_stats": file_stats
        }
        
    except Exception as e:
        logger.error(f"Error in process_parquet_directory: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def get_session_status_handler(session_id: str) -> Dict[str, Any]:
    """Handler for session status endpoint"""
    try:
        from session_manager import BatchSessionManager
        session_manager = BatchSessionManager()
        return session_manager.get_session_status(session_id)
    except Exception as e:
        logger.error(f"Error getting session status: {str(e)}")
        return {"error": str(e)}


def get_batch_status_handler(batch_id: str) -> Dict[str, Any]:
    """Handler for batch status endpoint"""
    try:
        from session_manager import BatchSessionManager
        session_manager = BatchSessionManager()
        
        # Extract task ID from batch ID (format: session_id_batch_remainder)
        parts = batch_id.split('_batch_')
        if len(parts) != 2:
            return {"error": "Invalid batch ID format"}
        
        session_id = parts[0]
        remainder = parts[1]
        
        # Get all tasks for the session and find the matching one
        tasks = session_manager.get_session_tasks(session_id)
        
        for task in tasks:
            if str(task.get('remainder')) == remainder:
                return task
        
        return {"error": "Batch not found"}
        
    except Exception as e:
        logger.error(f"Error getting batch status: {str(e)}")
        return {"error": str(e)}


def retry_batch_handler(batch_id: str) -> Dict[str, Any]:
    """Handler for retrying a failed batch"""
    try:
        from session_manager import BatchSessionManager
        from celery_worker import app as celery_app
        
        session_manager = BatchSessionManager()
        
        # Get batch details
        batch_details = get_batch_status_handler(batch_id)
        
        if batch_details.get("error"):
            return batch_details
        
        if batch_details.get("status") != "failed":
            return {"error": "Only failed batches can be retried"}
        
        # Get session details
        session_id = batch_details["session_id"]
        session_status = session_manager.get_session_status(session_id)
        
        if session_status.get("error"):
            return session_status
        
        # Reconstruct task data
        task_data = {
            "session_id": session_id,
            "file_path": batch_details["file_path"],
            "remainder": batch_details["remainder"],
            "total_batches": batch_details["total_batches"],
            "query_column": "hashed_query",  # Default
            "from_dialect": session_status["from_dialect"],
            "to_dialect": session_status["to_dialect"],
            "estimated_unique_per_batch": 0
        }
        
        # Resubmit to Celery
        task = celery_app.send_task(
            "process_modulo_batch",
            args=[task_data],
            queue='modulo_queue'
        )
        
        logger.info(f"Resubmitted batch {batch_id} as task {task.id}")
        
        return {
            "batch_id": batch_id,
            "new_task_id": task.id,
            "status": "resubmitted",
            "message": f"Batch {batch_id} has been resubmitted for processing"
        }
        
    except Exception as e:
        logger.error(f"Error retrying batch: {str(e)}")
        return {"error": str(e)}
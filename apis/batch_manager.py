"""
Scalable batch processing using ProcessPoolExecutor.

Handles large batch jobs (100K+ queries) without external dependencies.
Uses Python's ProcessPoolExecutor for parallel execution and file-based streaming.
"""

import logging
import uuid
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field, computed_field

from apis.config import get_transpiler_config

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """Job processing status"""
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobProgress(BaseModel):
    """Tracks progress and metadata for a batch job"""
    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(..., description="Current job status")
    total_queries: int = Field(..., description="Total number of queries to process")
    processed: int = Field(..., description="Number of queries processed so far")
    succeeded: int = Field(..., description="Number of queries that succeeded")
    failed: int = Field(..., description="Number of queries that failed")
    chunk_size: int = Field(..., description="Number of queries per processing chunk")
    created_at: datetime = Field(..., description="Job creation timestamp")
    started_at: Optional[datetime] = Field(None, description="Job start timestamp")
    completed_at: Optional[datetime] = Field(None, description="Job completion timestamp")
    error: Optional[str] = Field(None, description="Error message if job failed")

    @computed_field
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage"""
        if self.total_queries == 0:
            return 0.0
        return (self.processed / self.total_queries) * 100

    @computed_field
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.processed == 0:
            return 0.0
        return (self.succeeded / self.processed) * 100

    @computed_field
    @property
    def duration_ms(self) -> Optional[float]:
        """Calculate job duration in milliseconds"""
        if self.started_at is None:
            return None
        end_time = self.completed_at if self.completed_at else datetime.now()
        return (end_time - self.started_at).total_seconds() * 1000

    @computed_field
    @property
    def eta_ms(self) -> Optional[float]:
        """Estimate time remaining in milliseconds"""
        if self.started_at is None or self.processed == 0:
            return None
        elapsed_ms = (datetime.now() - self.started_at).total_seconds() * 1000
        queries_per_ms = self.processed / elapsed_ms
        remaining_queries = self.total_queries - self.processed
        return remaining_queries / queries_per_ms if queries_per_ms > 0 else None

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return self.model_dump(mode='json')


class BatchJobManager:
    """
    Manages batch processing jobs using ProcessPoolExecutor.

    Features:
    - Parallel processing using CPU cores
    - Streaming results to JSONL files
    - Real-time progress tracking
    - No memory accumulation
    """

    def __init__(self):
        config = get_transpiler_config()
        self.result_dir = Path(config.batch_result_dir)
        self.result_dir.mkdir(parents=True, exist_ok=True)

        # Create global process pool
        self.pool_size = config.batch_pool_size
        self.executor = ProcessPoolExecutor(max_workers=self.pool_size)

        # In-memory job tracking
        self.jobs: Dict[str, JobProgress] = {}

        logger.info(
            f"BatchJobManager initialized: pool_size={self.pool_size}, "
            f"result_dir={self.result_dir}"
        )

    def create_job(self, total_queries: int, chunk_size: int) -> str:
        """Create a new batch job and return job_id"""
        job_id = str(uuid.uuid4())

        progress = JobProgress(
            job_id=job_id,
            status=JobStatus.QUEUED,
            total_queries=total_queries,
            processed=0,
            succeeded=0,
            failed=0,
            chunk_size=chunk_size,
            created_at=datetime.now()
        )

        self.jobs[job_id] = progress
        logger.info(
            f"Created job {job_id}: {total_queries} queries, "
            f"chunk_size={chunk_size}"
        )
        return job_id

    def get_job_status(self, job_id: str) -> Optional[JobProgress]:
        """Get current job status"""
        return self.jobs.get(job_id)

    def get_all_jobs(self, limit: int = 100, offset: int = 0) -> List[JobProgress]:
        """Get all jobs with pagination"""
        all_jobs = sorted(
            self.jobs.values(),
            key=lambda j: j.created_at,
            reverse=True
        )
        return all_jobs[offset:offset + limit]

    def get_result_file(self, job_id: str) -> Path:
        """Get the result file path for a job"""
        return self.result_dir / f"{job_id}.jsonl"

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job (best-effort)"""
        if job_id in self.jobs:
            job = self.jobs[job_id]
            if job.status == JobStatus.PROCESSING:
                job.status = JobStatus.CANCELLED
                logger.info(f"Cancelled job {job_id}")
                return True
        return False

    def delete_job(self, job_id: str) -> bool:
        """Delete a job and its results"""
        if job_id not in self.jobs:
            return False

        # Delete result file
        result_file = self.get_result_file(job_id)
        if result_file.exists():
            result_file.unlink()

        # Remove from tracking
        del self.jobs[job_id]
        logger.info(f"Deleted job {job_id}")
        return True

    async def process_batch(
        self,
        job_id: str,
        queries: List[Dict],
        processor_func,
        **processor_kwargs
    ):
        """
        Process a batch of queries using ProcessPoolExecutor.

        Args:
            job_id: Job identifier
            queries: List of query dicts with 'id' and 'query' fields
            processor_func: Function to process each query (must be picklable)
            **processor_kwargs: Additional args passed to processor_func
        """
        job = self.jobs.get(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return

        try:
            job.status = JobStatus.PROCESSING
            job.started_at = datetime.now()

            result_file = self.get_result_file(job_id)

            # Chunk queries for parallel processing
            chunks = [
                queries[i:i + job.chunk_size]
                for i in range(0, len(queries), job.chunk_size)
            ]

            logger.info(
                f"Job {job_id}: Processing {len(queries)} queries "
                f"in {len(chunks)} chunks using {self.pool_size} workers"
            )

            # Submit all chunks to executor
            futures = {}
            for chunk_idx, chunk in enumerate(chunks):
                future = self.executor.submit(
                    _process_chunk_worker,
                    chunk,
                    processor_func,
                    processor_kwargs
                )
                futures[future] = chunk_idx

            # Process results as they complete (streaming)
            with open(result_file, 'w') as f:
                for future in as_completed(futures):
                    chunk_idx = futures[future]

                    try:
                        chunk_results = future.result()

                        # Write results to file (streaming)
                        for result in chunk_results:
                            f.write(json.dumps(result) + '\n')
                            f.flush()

                            # Update job progress
                            job.processed += 1
                            if result.get('status') == 'success':
                                job.succeeded += 1
                            else:
                                job.failed += 1

                        logger.debug(
                            f"Job {job_id}: Completed chunk {chunk_idx + 1}/{len(chunks)}"
                        )

                    except Exception as e:
                        logger.error(
                            f"Job {job_id}: Chunk {chunk_idx} failed: {e}",
                            exc_info=True
                        )
                        job.failed += len(chunks[chunk_idx])
                        job.processed += len(chunks[chunk_idx])

                    # Check if cancelled
                    if job.status == JobStatus.CANCELLED:
                        logger.info(f"Job {job_id} cancelled, stopping processing")
                        break

            # Mark as completed
            if job.status != JobStatus.CANCELLED:
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.now()
                duration_s = (job.completed_at - job.started_at).total_seconds()

                logger.info(
                    f"Job {job_id} completed: "
                    f"{job.succeeded} succeeded, {job.failed} failed, "
                    f"{duration_s:.2f}s"
                )

        except Exception as e:
            job.status = JobStatus.FAILED
            job.error = str(e)
            job.completed_at = datetime.now()
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)


def _process_chunk_worker(chunk, processor_func, processor_kwargs):
    """
    Worker function to process a chunk of queries.
    Must be at module level for pickling.

    Args:
        chunk: List of query dicts
        processor_func: Function to process each query
        processor_kwargs: Additional kwargs for processor_func

    Returns:
        List of result dicts
    """
    results = []

    for query_item in chunk:
        try:
            # Process single query
            result = processor_func(query_item, **processor_kwargs)
            results.append({
                'id': query_item['id'],
                'status': 'success',
                **result
            })
        except Exception as e:
            results.append({
                'id': query_item['id'],
                'status': 'error',
                'error': str(e)
            })

    return results


# Global singleton instance
_batch_manager: Optional[BatchJobManager] = None


def get_batch_manager() -> BatchJobManager:
    """Get or create the global BatchJobManager instance"""
    global _batch_manager
    if _batch_manager is None:
        _batch_manager = BatchJobManager()
    return _batch_manager

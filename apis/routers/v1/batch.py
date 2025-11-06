from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse
import logging
import sqlglot
from datetime import datetime
from typing import List, Dict, Any
from sqlglot.optimizer.qualify_columns import quote_identifiers

from apis.models.requests import BatchAnalyzeRequest, AnalyzeRequest, TranspileOptions
from apis.models.responses import (
    JobSubmitResponse,
    JobStatusResponse,
    JobListResponse,
    JobListItem,
    JobDeleteResponse,
)
from apis.batch_manager import get_batch_manager
from apis.utils.helpers import (
    strip_comment,
    normalize_unicode_spaces,
    replace_struct_in_query,
    ensure_select_from_values,
    set_cte_names_case_sensitively,
    transform_table_part,
    transform_catalog_schema_only,
    add_comment_to_query,
    extract_functions_from_query,
    categorize_functions,
    load_supported_functions,
    extract_udfs,
    unsupported_functionality_identifiers,
    extract_db_and_Table_names,
    extract_joins_from_query,
    extract_cte_n_subquery_list,
)
from apis.config import get_transpiler_config

router = APIRouter()
logger = logging.getLogger(__name__)


def _process_query_worker(query_item: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Worker function to process a single query.
    Must be at module level for pickling by ProcessPoolExecutor.

    This function replicates the core logic from analyze_inline endpoint
    in a synchronous, serializable manner for use in ProcessPoolExecutor.

    Args:
        query_item: Dict with 'id' and 'query' fields
        **kwargs: source_dialect, target_dialect, options

    Returns:
        Dict with analysis results
    """
    try:
        source_dialect = kwargs.get('source_dialect')
        target_dialect = kwargs.get('target_dialect')
        options_dict = kwargs.get('options', {})
        query_id = query_item['id']
        query = query_item['query']

        # Reconstruct options object
        options = TranspileOptions(**options_dict) if options_dict else TranspileOptions()

        # Get config
        config = get_transpiler_config()

        # Normalize and clean query
        if config.default_normalize_ascii:
            query = normalize_unicode_spaces(query)
        query, comment = strip_comment(query, "condenast")

        if not query.strip():
            raise ValueError("Empty query provided")

        # Parse query
        tree = sqlglot.parse_one(query, read=source_dialect, error_level=None)

        # Handle two-phase qualification if enabled
        if options.use_two_phase_qualification_scheme:
            if options.skip_e6_transpilation:
                transformed_query = transform_catalog_schema_only(query, source_dialect)
                transpiled = add_comment_to_query(transformed_query, comment)
                return {
                    'transpiled_query': transpiled,
                    'executable': True,
                    'functions': {'supported': [], 'unsupported': []},
                    'metadata': {
                        'tables': [],
                        'joins': [],
                        'ctes': [],
                        'subqueries': [],
                        'udfs': [],
                        'schemas': []
                    }
                }
            else:
                tree = transform_table_part(tree)

        # Load supported functions for dialects
        target_supported_functions = load_supported_functions(target_dialect)
        source_supported_functions = load_supported_functions(source_dialect)

        # Extract and categorize functions from source
        extracted_functions = extract_functions_from_query(query)
        supported, unsupported = categorize_functions(
            extracted_functions,
            source_supported_functions,
            target_supported_functions
        )

        # Extract UDFs and check for unsupported features
        udfs = extract_udfs(tree, set(extracted_functions))
        unsupported_features = unsupported_functionality_identifiers(tree)

        # Extract metadata
        tables = extract_db_and_Table_names(tree)
        joins = extract_joins_from_query(query)
        ctes, subqueries = extract_cte_n_subquery_list(tree)
        schemas = list(set(table.split('.')[0] for table in tables if '.' in table))

        # Transpile
        tree = ensure_select_from_values(tree)
        tree = set_cte_names_case_sensitively(tree)
        transpiled = tree.sql(dialect=target_dialect, pretty=options.pretty_print)

        # Post-processing
        transpiled = replace_struct_in_query(transpiled)
        transpiled = add_comment_to_query(transpiled, comment)

        # Quote identifiers if enabled
        if config.enable_identifier_quoting:
            transpiled_tree = sqlglot.parse_one(transpiled, read=target_dialect, error_level=None)
            transpiled_tree = quote_identifiers(transpiled_tree, dialect=target_dialect)
            transpiled = transpiled_tree.sql(dialect=target_dialect, pretty=options.pretty_print)

        # Determine executability
        executable = len(unsupported) == 0 and len(unsupported_features) == 0

        return {
            'transpiled_query': transpiled,
            'executable': executable,
            'functions': {
                'supported': sorted(list(supported)),
                'unsupported': sorted(list(unsupported))
            },
            'metadata': {
                'tables': sorted(list(tables)),
                'joins': joins,
                'ctes': ctes,
                'subqueries': subqueries,
                'udfs': sorted(list(udfs)),
                'schemas': sorted(list(schemas))
            }
        }

    except Exception as e:
        # Return error in a format that can be serialized
        raise Exception(f"Query processing failed: {str(e)}")


async def _process_batch_from_s3(
    job_id: str,
    s3_uri: str,
    chunk_size: int,
    s3_config: Dict[str, str],
    processor_kwargs: Dict[str, Any]
):
    """
    Background task to process batch from S3 Parquet file.

    Streams chunks from S3 and submits them to ProcessPoolExecutor.
    """
    from apis.batch_manager import get_batch_manager, stream_parquet_from_s3
    from concurrent.futures import as_completed

    batch_manager = get_batch_manager()
    job = batch_manager.get_job_status(job_id)

    if not job:
        logger.error(f"Job {job_id} not found")
        return

    try:
        job.status = batch_manager.jobs[job_id].status.__class__.PROCESSING
        job.started_at = datetime.now()

        result_file = batch_manager.get_result_file(job_id)

        logger.info(f"Job {job_id}: Starting S3 Parquet streaming from {s3_uri}")

        # Stream chunks from S3 and submit to executor
        futures = {}
        chunk_idx = 0

        with open(result_file, 'w') as f:
            for queries_chunk in stream_parquet_from_s3(
                s3_uri=s3_uri,
                chunk_size=chunk_size,
                s3_endpoint_url=s3_config['endpoint_url'],
                s3_access_key_id=s3_config['access_key_id'],
                s3_secret_access_key=s3_config['secret_access_key'],
                s3_region=s3_config['region']
            ):
                # Submit chunk to executor
                future = batch_manager.executor.submit(
                    _process_chunk_worker,
                    queries_chunk,
                    _process_query_worker,
                    processor_kwargs
                )
                futures[future] = chunk_idx
                chunk_idx += 1

                logger.debug(f"Job {job_id}: Submitted chunk {chunk_idx}")

            # Process results as they complete
            for future in as_completed(futures):
                chunk_num = futures[future]

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

                    logger.debug(f"Job {job_id}: Completed chunk {chunk_num + 1}")

                except Exception as e:
                    logger.error(f"Job {job_id}: Chunk {chunk_num} failed: {e}", exc_info=True)
                    # Mark all queries in chunk as failed
                    job.failed += chunk_size
                    job.processed += chunk_size

                # Check if cancelled
                if job.status.value == 'cancelled':
                    logger.info(f"Job {job_id} cancelled, stopping processing")
                    break

        # Mark as completed
        if job.status.value != 'cancelled':
            job.status = batch_manager.jobs[job_id].status.__class__.COMPLETED
            job.completed_at = datetime.now()
            duration_s = (job.completed_at - job.started_at).total_seconds()

            logger.info(
                f"Job {job_id} completed: "
                f"{job.succeeded} succeeded, {job.failed} failed, "
                f"{duration_s:.2f}s"
            )

    except Exception as e:
        job.status = batch_manager.jobs[job_id].status.__class__.FAILED
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
    from apis.batch_manager import _process_chunk_worker as original_worker
    # Delegate to the original worker in batch_manager
    return original_worker(chunk, processor_func, processor_kwargs)


@router.post("/analyze", response_model=JobSubmitResponse)
async def analyze_batch(request: BatchAnalyzeRequest, background_tasks: BackgroundTasks):
    """
    Submit a batch of SQL queries from S3 Parquet file for asynchronous analysis.

    The Parquet file must contain columns: 'id' (string) and 'query' (string).

    Returns immediately with a job_id that can be used to:
    - Check job status/progress: GET /batch/jobs/{job_id}
    - Retrieve results: GET /batch/jobs/{job_id}/results
    - Cancel job: DELETE /batch/jobs/{job_id}

    Example:
        {
            "s3_uri": "s3://my-bucket/queries.parquet",
            "source_dialect": "snowflake",
            "target_dialect": "e6",
            "chunk_size": 1000
        }
    """
    from apis.batch_manager import stream_parquet_from_s3
    from apis.config import get_transpiler_config

    batch_manager = get_batch_manager()
    config = get_transpiler_config()

    # First, get the total row count from Parquet file
    # This requires a quick scan to create the job with correct total
    import polars as pl

    storage_options = {
        "aws_endpoint_url": config.s3_endpoint_url,
        "aws_access_key_id": config.s3_access_key_id,
        "aws_secret_access_key": config.s3_secret_access_key,
        "aws_region": config.s3_region,
        "aws_allow_http": "true" if "localhost" in config.s3_endpoint_url or "127.0.0.1" in config.s3_endpoint_url else "false"
    }

    try:
        # Quick count to get total queries
        lf = pl.scan_parquet(request.s3_uri, storage_options=storage_options)
        total_queries = lf.select(pl.count()).collect().item()
        logger.info(f"Parquet file {request.s3_uri} contains {total_queries} queries")
    except Exception as e:
        logger.error(f"Failed to read Parquet file {request.s3_uri}: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read Parquet file from S3: {str(e)}"
        )

    # Create job
    job_id = batch_manager.create_job(
        total_queries=total_queries,
        chunk_size=request.chunk_size
    )

    # Prepare processor kwargs
    processor_kwargs = {
        'source_dialect': request.source_dialect,
        'target_dialect': request.target_dialect,
        'options': request.options.dict() if request.options else {},
    }

    # Submit job for background processing with S3 streaming
    background_tasks.add_task(
        _process_batch_from_s3,
        job_id=job_id,
        s3_uri=request.s3_uri,
        chunk_size=request.chunk_size,
        s3_config={
            'endpoint_url': config.s3_endpoint_url,
            'access_key_id': config.s3_access_key_id,
            'secret_access_key': config.s3_secret_access_key,
            'region': config.s3_region
        },
        processor_kwargs=processor_kwargs
    )

    # Get job info to return
    job = batch_manager.get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=500, detail="Failed to create job")

    logger.info(
        f"Created batch job {job_id} for S3 file {request.s3_uri}: "
        f"{total_queries} queries, chunk_size={request.chunk_size}"
    )

    return JobSubmitResponse(
        job_id=job.job_id,
        status=job.status.value,
        total_queries=job.total_queries,
        chunk_size=job.chunk_size,
        created_at=job.created_at.isoformat()
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get the status and progress of a batch job.

    Returns detailed information about the job including:
    - Current status (queued/processing/completed/failed/cancelled)
    - Progress metrics (processed, succeeded, failed)
    - Timing information (duration, ETA)
    - Success rate
    """
    batch_manager = get_batch_manager()
    job = batch_manager.get_job_status(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    job_dict = job.to_dict()
    return JobStatusResponse(**job_dict)


@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    limit: int = Query(default=100, ge=1, le=1000, description="Number of jobs to return"),
    offset: int = Query(default=0, ge=0, description="Offset for pagination")
):
    """
    List all batch jobs with pagination.

    Jobs are sorted by creation time (most recent first).
    Use limit and offset parameters for pagination.
    """
    batch_manager = get_batch_manager()
    jobs = batch_manager.get_all_jobs(limit=limit, offset=offset)

    job_items = [
        JobListItem(
            job_id=job.job_id,
            status=job.status.value,
            total_queries=job.total_queries,
            processed=job.processed,
            succeeded=job.succeeded,
            failed=job.failed,
            created_at=job.created_at.isoformat(),
            progress_percentage=job.progress_percentage
        )
        for job in jobs
    ]

    return JobListResponse(
        jobs=job_items,
        total=len(job_items),
        limit=limit,
        offset=offset
    )


@router.get("/jobs/{job_id}/results")
async def get_job_results(job_id: str):
    """
    Download the results of a completed batch job as a JSONL file.

    Each line in the file is a JSON object representing one query result.
    The file can be streamed line-by-line to avoid loading the entire result set in memory.

    Returns 404 if job doesn't exist or results file is not available yet.
    """
    batch_manager = get_batch_manager()
    job = batch_manager.get_job_status(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    result_file = batch_manager.get_result_file(job_id)
    if not result_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Results not available yet for job {job_id}. Check job status first."
        )

    return FileResponse(
        path=str(result_file),
        media_type="application/x-ndjson",
        filename=f"{job_id}.jsonl"
    )


@router.delete("/jobs/{job_id}", response_model=JobDeleteResponse)
async def delete_job(job_id: str):
    """
    Delete a batch job and its results.

    If the job is currently processing, it will be marked as cancelled.
    The job record and result file will be removed.

    Returns 404 if the job doesn't exist.
    """
    batch_manager = get_batch_manager()

    # Try to cancel first if processing
    batch_manager.cancel_job(job_id)

    # Then delete
    deleted = batch_manager.delete_job(job_id)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    logger.info(f"Deleted batch job {job_id}")

    return JobDeleteResponse(
        job_id=job_id,
        deleted=True
    )

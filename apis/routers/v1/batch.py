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


@router.post("/analyze", response_model=JobSubmitResponse)
async def analyze_batch(request: BatchAnalyzeRequest, background_tasks: BackgroundTasks):
    """
    Submit a batch of SQL queries for asynchronous analysis.

    Creates a background job that processes queries in parallel using ProcessPoolExecutor.
    Returns immediately with a job_id that can be used to:
    - Check job status/progress: GET /batch/jobs/{job_id}
    - Retrieve results: GET /batch/jobs/{job_id}/results
    - Cancel job: DELETE /batch/jobs/{job_id}
    """
    batch_manager = get_batch_manager()

    # Create job
    job_id = batch_manager.create_job(
        total_queries=len(request.queries),
        chunk_size=request.chunk_size
    )

    # Convert queries to dict format for worker
    queries_dict = [
        {'id': q.id, 'query': q.query}
        for q in request.queries
    ]

    # Prepare processor kwargs (these will be passed to each worker)
    processor_kwargs = {
        'source_dialect': request.source_dialect,
        'target_dialect': request.target_dialect,
        'options': request.options.dict() if request.options else {},
    }

    # Submit job for background processing
    background_tasks.add_task(
        batch_manager.process_batch,
        job_id=job_id,
        queries=queries_dict,
        processor_func=_process_query_worker,
        **processor_kwargs
    )

    # Get job info to return
    job = batch_manager.get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=500, detail="Failed to create job")

    logger.info(
        f"Created batch job {job_id}: {len(request.queries)} queries, "
        f"chunk_size={request.chunk_size}"
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

from fastapi import APIRouter, HTTPException
import logging
from datetime import datetime
from typing import List

from apis.models.requests import BatchAnalyzeRequest
from apis.models.responses import (
    BatchAnalyzeResponse,
    BatchAnalysisResultItem,
    BatchSummary,
    QueryStatus,
    ErrorDetail,
    FunctionAnalysis,
    QueryMetadata,
)
from apis.routers.v1.inline import analyze_inline
from apis.models.requests import AnalyzeRequest

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/analyze", response_model=BatchAnalyzeResponse)
async def analyze_batch(request: BatchAnalyzeRequest):
    """
    Analyze multiple SQL queries in batch.

    Processes each query and returns transpilation + detailed analysis metadata.
    Can optionally stop on first error.
    """
    start_time = datetime.now()
    results: List[BatchAnalysisResultItem] = []
    succeeded = 0
    failed = 0
    executable_count = 0

    # Aggregate tracking
    all_supported_functions = set()
    all_unsupported_functions = set()
    all_udfs = set()
    all_tables = set()
    all_schemas = set()
    total_joins = 0
    total_ctes = 0
    total_subqueries = 0

    # Timing tracking
    query_durations = []
    parsing_times = []
    transpilation_times = []
    function_analysis_times = []

    # Complexity tracking
    tables_per_query = []
    functions_per_query = []

    logger.info(f"Starting batch analysis of {len(request.queries)} queries")

    for query_item in request.queries:
        try:
            # Create individual analyze request
            analyze_req = AnalyzeRequest(
                query=query_item.query,
                source_dialect=request.source_dialect,
                target_dialect=request.target_dialect,
                query_id=query_item.id,
                options=request.options,
            )

            # Analyze using inline endpoint
            response = await analyze_inline(analyze_req)

            results.append(
                BatchAnalysisResultItem(
                    id=query_item.id,
                    status=QueryStatus.SUCCESS,
                    transpiled_query=response.transpiled_query,
                    executable=response.executable,
                    functions=response.functions,
                    metadata=response.metadata,
                )
            )
            succeeded += 1

            # Aggregate statistics
            if response.executable:
                executable_count += 1

            # Function aggregates
            all_supported_functions.update(response.functions.supported)
            all_unsupported_functions.update(response.functions.unsupported)
            all_udfs.update(response.metadata.udfs)

            # Metadata aggregates
            all_tables.update(response.metadata.tables)
            all_schemas.update(response.metadata.schemas)
            total_joins += len(response.metadata.joins)
            total_ctes += len(response.metadata.ctes)
            total_subqueries += len(response.metadata.subqueries)

            # Timing aggregates
            if response.timing:
                query_durations.append(response.timing.total_ms)
                if response.timing.parsing_ms is not None:
                    parsing_times.append(response.timing.parsing_ms)
                if response.timing.transpilation_ms is not None:
                    transpilation_times.append(response.timing.transpilation_ms)
                if response.timing.function_analysis_ms is not None:
                    function_analysis_times.append(response.timing.function_analysis_ms)

            # Complexity tracking
            tables_per_query.append(len(response.metadata.tables))
            total_functions = len(response.functions.supported) + len(response.functions.unsupported)
            functions_per_query.append(total_functions)

        except HTTPException as e:
            error_detail = ErrorDetail(
                message=e.detail,
                code=str(e.status_code),
            )
            results.append(
                BatchAnalysisResultItem(
                    id=query_item.id,
                    status=QueryStatus.ERROR,
                    error=error_detail,
                )
            )
            failed += 1

            logger.warning(f"Query {query_item.id} failed: {e.detail}")

            if request.stop_on_error:
                logger.info(f"Stopping batch processing due to error in query {query_item.id}")
                break

        except Exception as e:
            error_detail = ErrorDetail(
                message=str(e),
                code="INTERNAL_ERROR",
            )
            results.append(
                BatchAnalysisResultItem(
                    id=query_item.id,
                    status=QueryStatus.ERROR,
                    error=error_detail,
                )
            )
            failed += 1

            logger.error(f"Query {query_item.id} failed with unexpected error: {str(e)}", exc_info=True)

            if request.stop_on_error:
                logger.info(f"Stopping batch processing due to error in query {query_item.id}")
                break

    duration_ms = (datetime.now() - start_time).total_seconds() * 1000

    # Calculate summary statistics
    total_queries = len(results)
    non_executable_count = succeeded - executable_count
    success_rate = (succeeded / total_queries * 100) if total_queries > 0 else 0.0

    # Timing calculations
    avg_query_duration = sum(query_durations) / len(query_durations) if query_durations else 0.0
    min_query_duration = min(query_durations) if query_durations else 0.0
    max_query_duration = max(query_durations) if query_durations else 0.0
    avg_parsing = sum(parsing_times) / len(parsing_times) if parsing_times else None
    avg_transpilation = sum(transpilation_times) / len(transpilation_times) if transpilation_times else None
    avg_function_analysis = sum(function_analysis_times) / len(function_analysis_times) if function_analysis_times else None

    # Complexity calculations
    avg_tables = sum(tables_per_query) / len(tables_per_query) if tables_per_query else 0.0
    avg_functions = sum(functions_per_query) / len(functions_per_query) if functions_per_query else 0.0

    logger.info(
        f"Batch analysis completed: {succeeded} succeeded, {failed} failed, {executable_count} executable, {duration_ms:.2f}ms"
    )

    from apis.models.responses import (
        ExecutionSummary,
        FunctionSummary,
        ComplexitySummary,
        TimingSummary,
        DialectSummary,
    )

    return BatchAnalyzeResponse(
        results=results,
        summary=BatchSummary(
            execution=ExecutionSummary(
                total_queries=total_queries,
                succeeded=succeeded,
                failed=failed,
                executable_queries=executable_count,
                non_executable_queries=non_executable_count,
                success_rate_percentage=success_rate,
            ),
            functions=FunctionSummary(
                unique_supported_count=len(all_supported_functions),
                unique_unsupported_count=len(all_unsupported_functions),
                total_udfs=len(all_udfs),
            ),
            complexity=ComplexitySummary(
                total_unique_tables=len(all_tables),
                total_unique_schemas=len(all_schemas),
                avg_tables_per_query=avg_tables,
                avg_functions_per_query=avg_functions,
                total_joins=total_joins,
                total_ctes=total_ctes,
                total_subqueries=total_subqueries,
            ),
            timing=TimingSummary(
                total_duration_ms=duration_ms,
                avg_query_duration_ms=avg_query_duration,
                min_query_duration_ms=min_query_duration,
                max_query_duration_ms=max_query_duration,
                avg_parsing_ms=avg_parsing,
                avg_transpilation_ms=avg_transpilation,
                avg_function_analysis_ms=avg_function_analysis,
            ),
            dialects=DialectSummary(
                source_dialect=request.source_dialect,
                target_dialect=request.target_dialect,
            ),
        ),
    )

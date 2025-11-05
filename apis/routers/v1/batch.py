from fastapi import APIRouter, HTTPException
import logging
from datetime import datetime
from typing import List

from apis.models.requests import BatchTranspileRequest, BatchAnalyzeRequest
from apis.models.responses import (
    BatchTranspileResponse,
    BatchAnalyzeResponse,
    BatchResultItem,
    BatchAnalysisResultItem,
    BatchSummary,
    QueryStatus,
    ErrorDetail,
    FunctionAnalysis,
    QueryMetadata,
)
from apis.routers.v1.inline import transpile_inline, analyze_inline
from apis.models.requests import TranspileRequest, AnalyzeRequest

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/transpile", response_model=BatchTranspileResponse)
async def transpile_batch(request: BatchTranspileRequest):
    """
    Transpile multiple SQL queries in batch.

    Processes each query and returns individual results with summary.
    Can optionally stop on first error.
    """
    start_time = datetime.now()
    results: List[BatchResultItem] = []
    succeeded = 0
    failed = 0

    logger.info(f"Starting batch transpilation of {len(request.queries)} queries")

    for query_item in request.queries:
        try:
            # Create individual transpile request
            transpile_req = TranspileRequest(
                query=query_item.query,
                source_dialect=request.source_dialect,
                target_dialect=request.target_dialect,
                query_id=query_item.id,
                options=request.options,
            )

            # Transpile using inline endpoint
            response = await transpile_inline(transpile_req)

            results.append(
                BatchResultItem(
                    id=query_item.id,
                    status=QueryStatus.SUCCESS,
                    transpiled_query=response.transpiled_query,
                )
            )
            succeeded += 1

        except HTTPException as e:
            error_detail = ErrorDetail(
                message=e.detail,
                code=str(e.status_code),
            )
            results.append(
                BatchResultItem(
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
                BatchResultItem(
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

    logger.info(
        f"Batch transpilation completed: {succeeded} succeeded, {failed} failed, {duration_ms:.2f}ms"
    )

    return BatchTranspileResponse(
        results=results,
        summary=BatchSummary(
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            duration_ms=duration_ms,
        ),
    )


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

    logger.info(
        f"Batch analysis completed: {succeeded} succeeded, {failed} failed, {duration_ms:.2f}ms"
    )

    return BatchAnalyzeResponse(
        results=results,
        summary=BatchSummary(
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            duration_ms=duration_ms,
        ),
    )

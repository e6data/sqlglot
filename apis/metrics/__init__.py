"""
Prometheus metrics for the SQLGlot transpiler service.

This module defines all Prometheus metrics used to monitor the transpiler's
performance, usage patterns, and errors. Metrics are conditionally collected
based on the E6_ENABLE_METRICS configuration.
"""

import logging
from typing import Optional
from prometheus_client import Counter, Histogram, Gauge, Info, CollectorRegistry, REGISTRY
from apis.config import get_transpiler_config

logger = logging.getLogger(__name__)

# Get configuration
config = get_transpiler_config()

# Parse histogram buckets from config
def parse_histogram_buckets(buckets_str: str) -> list[float]:
    """Parse comma-separated histogram bucket values."""
    try:
        return [float(b.strip()) for b in buckets_str.split(",")]
    except (ValueError, AttributeError):
        logger.warning(f"Invalid histogram buckets '{buckets_str}', using defaults")
        return [0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]

HISTOGRAM_BUCKETS = parse_histogram_buckets(config.metrics_histogram_buckets)

# =============================================================================
# HTTP Metrics
# =============================================================================

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"],
    registry=REGISTRY
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=HISTOGRAM_BUCKETS,
    registry=REGISTRY
)

http_requests_in_progress = Gauge(
    "http_requests_in_progress",
    "Number of HTTP requests currently being processed",
    ["method", "endpoint"],
    registry=REGISTRY
)

# =============================================================================
# Transpilation Metrics
# =============================================================================

transpiler_queries_total = Counter(
    "transpiler_queries_total",
    "Total queries processed",
    ["endpoint", "source_dialect", "target_dialect", "status"],
    registry=REGISTRY
)

transpiler_query_size_bytes = Histogram(
    "transpiler_query_size_bytes",
    "Query size in bytes",
    ["source_dialect", "target_dialect"],
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
    registry=REGISTRY
)

transpiler_executable_queries_total = Counter(
    "transpiler_executable_queries_total",
    "Total queries by executability status",
    ["source_dialect", "target_dialect", "executable"],
    registry=REGISTRY
)

transpiler_errors_total = Counter(
    "transpiler_errors_total",
    "Total transpilation errors",
    ["endpoint", "error_type", "source_dialect", "target_dialect"],
    registry=REGISTRY
)

# =============================================================================
# Function Analysis Metrics
# =============================================================================

transpiler_supported_functions_count = Histogram(
    "transpiler_supported_functions_count",
    "Number of supported functions per query",
    ["source_dialect", "target_dialect"],
    buckets=[0, 1, 2, 5, 10, 20, 50, 100, 200],
    registry=REGISTRY
)

transpiler_unsupported_functions_count = Histogram(
    "transpiler_unsupported_functions_count",
    "Number of unsupported functions per query",
    ["source_dialect", "target_dialect"],
    buckets=[0, 1, 2, 5, 10, 20, 50],
    registry=REGISTRY
)

transpiler_udf_count = Histogram(
    "transpiler_udf_count",
    "Number of user-defined functions per query",
    ["source_dialect", "target_dialect"],
    buckets=[0, 1, 2, 5, 10, 20, 50],
    registry=REGISTRY
)

# =============================================================================
# Query Complexity Metrics
# =============================================================================

transpiler_query_tables_count = Histogram(
    "transpiler_query_tables_count",
    "Number of tables referenced per query",
    ["source_dialect", "target_dialect"],
    buckets=[0, 1, 2, 5, 10, 20, 50, 100],
    registry=REGISTRY
)

transpiler_query_joins_count = Histogram(
    "transpiler_query_joins_count",
    "Number of joins per query",
    ["source_dialect", "target_dialect"],
    buckets=[0, 1, 2, 5, 10, 20, 50],
    registry=REGISTRY
)

transpiler_query_ctes_count = Histogram(
    "transpiler_query_ctes_count",
    "Number of CTEs per query",
    ["source_dialect", "target_dialect"],
    buckets=[0, 1, 2, 5, 10, 20],
    registry=REGISTRY
)

transpiler_query_subqueries_count = Histogram(
    "transpiler_query_subqueries_count",
    "Number of subqueries per query",
    ["source_dialect", "target_dialect"],
    buckets=[0, 1, 2, 5, 10, 20, 50],
    registry=REGISTRY
)

# =============================================================================
# Phase Timing Metrics
# =============================================================================

transpiler_phase_duration_seconds = Histogram(
    "transpiler_phase_duration_seconds",
    "Duration of individual transpilation phases in seconds",
    ["phase", "source_dialect", "target_dialect"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
    registry=REGISTRY
)

transpiler_aggregate_phase_duration_seconds = Histogram(
    "transpiler_aggregate_phase_duration_seconds",
    "Duration of aggregate phase groups in seconds",
    ["phase_group", "source_dialect", "target_dialect"],
    buckets=HISTOGRAM_BUCKETS,
    registry=REGISTRY
)

# =============================================================================
# Batch Processing Metrics
# =============================================================================

transpiler_batch_requests_total = Counter(
    "transpiler_batch_requests_total",
    "Total batch requests processed",
    ["source_dialect", "target_dialect"],
    registry=REGISTRY
)

transpiler_batch_size = Histogram(
    "transpiler_batch_size",
    "Number of queries per batch request",
    ["source_dialect", "target_dialect"],
    buckets=[1, 5, 10, 50, 100, 500, 1000, 5000, 10000],
    registry=REGISTRY
)

transpiler_batch_duration_seconds = Histogram(
    "transpiler_batch_duration_seconds",
    "Batch request duration in seconds",
    ["source_dialect", "target_dialect"],
    buckets=HISTOGRAM_BUCKETS,
    registry=REGISTRY
)

transpiler_batch_queries_processed = Counter(
    "transpiler_batch_queries_processed",
    "Total queries processed in batch mode",
    ["source_dialect", "target_dialect", "status"],
    registry=REGISTRY
)

transpiler_batch_success_rate = Gauge(
    "transpiler_batch_success_rate",
    "Success rate for the most recent batch (0.0 to 1.0)",
    ["source_dialect", "target_dialect"],
    registry=REGISTRY
)

# =============================================================================
# System/Application Metrics
# =============================================================================

transpiler_build_info = Info(
    "transpiler_build",
    "Transpiler build information",
    registry=REGISTRY
)

transpiler_worker_count = Gauge(
    "transpiler_worker_count",
    "Number of configured worker processes",
    registry=REGISTRY
)

transpiler_starts_total = Counter(
    "transpiler_starts_total",
    "Number of times the transpiler has been started",
    registry=REGISTRY
)

# =============================================================================
# Helper Functions
# =============================================================================

def record_http_request(method: str, endpoint: str, status_code: int, duration_seconds: float):
    """Record HTTP-level metrics."""
    if not config.enable_metrics:
        return

    try:
        http_requests_total.labels(
            method=method,
            endpoint=endpoint,
            status_code=str(status_code)
        ).inc()

        http_request_duration_seconds.labels(
            method=method,
            endpoint=endpoint
        ).observe(duration_seconds)
    except Exception as e:
        logger.warning(f"Failed to record HTTP metrics: {e}")


def record_transpilation(
    endpoint: str,
    source_dialect: str,
    target_dialect: str,
    status: str,
    query_size_bytes: Optional[int] = None,
    error_type: Optional[str] = None
):
    """Record transpilation-specific metrics."""
    if not config.enable_metrics:
        return

    try:
        transpiler_queries_total.labels(
            endpoint=endpoint,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            status=status
        ).inc()

        if query_size_bytes is not None:
            transpiler_query_size_bytes.labels(
                source_dialect=source_dialect,
                target_dialect=target_dialect
            ).observe(query_size_bytes)

        if error_type:
            transpiler_errors_total.labels(
                endpoint=endpoint,
                error_type=error_type,
                source_dialect=source_dialect,
                target_dialect=target_dialect
            ).inc()
    except Exception as e:
        logger.warning(f"Failed to record transpilation metrics: {e}")


def record_analysis(
    source_dialect: str,
    target_dialect: str,
    executable: bool,
    query_size_bytes: int,
    supported_functions_count: int,
    unsupported_functions_count: int,
    udf_count: int,
    tables_count: int,
    joins_count: int,
    ctes_count: int,
    subqueries_count: int,
    timings: Optional[dict] = None
):
    """Record detailed analysis metrics."""
    if not config.enable_metrics:
        return

    try:
        # Executability
        transpiler_executable_queries_total.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            executable=str(executable).lower()
        ).inc()

        # Query size
        transpiler_query_size_bytes.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(query_size_bytes)

        # Function analysis
        transpiler_supported_functions_count.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(supported_functions_count)

        transpiler_unsupported_functions_count.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(unsupported_functions_count)

        transpiler_udf_count.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(udf_count)

        # Query complexity
        transpiler_query_tables_count.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(tables_count)

        transpiler_query_joins_count.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(joins_count)

        transpiler_query_ctes_count.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(ctes_count)

        transpiler_query_subqueries_count.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(subqueries_count)

        # Phase timings (if enabled and provided)
        if config.metrics_track_phase_timings and timings:
            # Individual phase timings
            phase_mapping = {
                'normalization_ms': 'normalization',
                'config_loading_ms': 'config_loading',
                'parsing_ms': 'parsing',
                'function_extraction_ms': 'function_extraction',
                'function_categorization_ms': 'function_categorization',
                'udf_extraction_ms': 'udf_extraction',
                'unsupported_detection_ms': 'unsupported_detection',
                'table_extraction_ms': 'table_extraction',
                'join_extraction_ms': 'join_extraction',
                'cte_extraction_ms': 'cte_extraction',
                'schema_extraction_ms': 'schema_extraction',
                'ast_preprocessing_ms': 'ast_preprocessing',
                'transpilation_parsing_ms': 'transpilation_parsing',
                'identifier_qualification_ms': 'identifier_qualification',
                'sql_generation_ms': 'sql_generation',
                'post_processing_ms': 'post_processing',
                'transpiled_parsing_ms': 'transpiled_parsing',
                'transpiled_function_extraction_ms': 'transpiled_function_extraction',
                'transpiled_function_analysis_ms': 'transpiled_function_analysis',
                'ast_serialization_ms': 'ast_serialization',
            }

            for timing_key, phase_name in phase_mapping.items():
                if timing_key in timings:
                    duration_seconds = timings[timing_key] / 1000.0
                    transpiler_phase_duration_seconds.labels(
                        phase=phase_name,
                        source_dialect=source_dialect,
                        target_dialect=target_dialect
                    ).observe(duration_seconds)

            # Aggregate phase groups
            aggregate_mapping = {
                'function_analysis_ms': 'function_analysis',
                'metadata_extraction_ms': 'metadata_extraction',
                'transpilation_ms': 'transpilation',
                'post_analysis_ms': 'post_analysis',
            }

            for timing_key, group_name in aggregate_mapping.items():
                if timing_key in timings:
                    duration_seconds = timings[timing_key] / 1000.0
                    transpiler_aggregate_phase_duration_seconds.labels(
                        phase_group=group_name,
                        source_dialect=source_dialect,
                        target_dialect=target_dialect
                    ).observe(duration_seconds)

    except Exception as e:
        logger.warning(f"Failed to record analysis metrics: {e}")


def record_batch(
    source_dialect: str,
    target_dialect: str,
    batch_size: int,
    duration_seconds: float,
    succeeded: int,
    failed: int,
    success_rate: float
):
    """Record batch processing metrics."""
    if not config.enable_metrics:
        return

    try:
        transpiler_batch_requests_total.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).inc()

        transpiler_batch_size.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(batch_size)

        transpiler_batch_duration_seconds.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).observe(duration_seconds)

        transpiler_batch_queries_processed.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            status="success"
        ).inc(succeeded)

        transpiler_batch_queries_processed.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            status="failure"
        ).inc(failed)

        transpiler_batch_success_rate.labels(
            source_dialect=source_dialect,
            target_dialect=target_dialect
        ).set(success_rate)

    except Exception as e:
        logger.warning(f"Failed to record batch metrics: {e}")


def initialize_metrics():
    """Initialize metrics at startup."""
    if not config.enable_metrics:
        logger.info("Metrics collection is disabled")
        return

    try:
        # Set build info
        import sys
        import fastapi

        transpiler_build_info.info({
            "version": "1.0.0",
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "fastapi_version": fastapi.__version__
        })

        # Set worker count
        transpiler_worker_count.set(config.uvicorn_workers)

        # Increment start counter
        transpiler_starts_total.inc()

        logger.info(f"Prometheus metrics initialized (endpoint: {config.metrics_endpoint})")
    except Exception as e:
        logger.error(f"Failed to initialize metrics: {e}")

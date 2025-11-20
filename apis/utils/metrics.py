"""
Prometheus metrics module for converter API.

This module provides metrics collection and reporting functionality
for monitoring SQL conversion performance and errors.
"""

import os
import logging

# Initialize logger
logger = logging.getLogger(__name__)

# Flag to track if Prometheus is available
PROMETHEUS_AVAILABLE = True

try:
    from prometheus_client import (
        Counter,
        Histogram,
        CollectorRegistry,
        multiprocess,
        generate_latest,
        CONTENT_TYPE_LATEST,
        REGISTRY,
    )
except ImportError:
    PROMETHEUS_AVAILABLE = False
    REGISTRY = None
    logger.warning("prometheus_client not installed. Metrics will be disabled.")

# ==================== Prometheus Setup ====================
if PROMETHEUS_AVAILABLE:
    try:
        PROMETHEUS_MULTIPROC_DIR = os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc_dir")
        if not os.path.exists(PROMETHEUS_MULTIPROC_DIR):
            os.makedirs(PROMETHEUS_MULTIPROC_DIR, exist_ok=True)

        # Use the default registry for recording metrics in multiprocess mode
        # Metrics will be written to PROMETHEUS_MULTIPROC_DIR automatically
        registry = REGISTRY
    except Exception as e:
        logger.error(f"Failed to initialize Prometheus registry: {e}")
        PROMETHEUS_AVAILABLE = False
        registry = None
else:
    registry = None

# ==================== Metric Definitions ====================

if PROMETHEUS_AVAILABLE and registry:
    # Request counters
    total_queries = Counter(
        "total_queries",
        "Total number of requests",
        ["from_dialect", "to_dialect", "status"],
        registry=registry,
    )

    total_errors = Counter(
        "total_errors",
        "Total number of errors",
        ["from_dialect", "to_dialect", "error_type"],
        registry=registry,
    )

    # Duration histogram
    total_time_by_query = Histogram(
        "total_time_by_query",
        "Duration of requests in seconds",
        ["from_dialect", "to_dialect"],
        buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
        registry=registry,
    )

    # Process duration histograms
    total_time_by_process = Histogram(
        "total_time_by_process",
        "Duration of individual processing steps",
        ["step_name", "from_dialect", "to_dialect"],
        buckets=[0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
        registry=registry,
    )

    # Query characteristics
    query_size_bytes = Histogram(
        "query_size_bytes",
        "Size of input queries in bytes",
        ["from_dialect", "to_dialect"],
        buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
        registry=registry,
    )

    # ==================== Stats API Metrics ====================

    # Stats request counters
    stats_total_requests = Counter(
        "stats_total_requests",
        "Total number of statistics API requests",
        ["from_dialect", "to_dialect", "status"],
        registry=registry,
    )

    stats_total_errors = Counter(
        "stats_total_errors",
        "Total number of statistics API errors",
        ["from_dialect", "to_dialect", "error_type"],
        registry=registry,
    )

    # Stats function counters
    stats_function_counts = Counter(
        "stats_function_counts",
        "Count of functions by type (supported/unsupported/udf)",
        ["from_dialect", "to_dialect", "function_type"],
        registry=registry,
    )

    # Stats executable query counter
    stats_executable_queries = Counter(
        "stats_executable_queries",
        "Count of executable vs non-executable queries",
        ["from_dialect", "to_dialect", "executable"],
        registry=registry,
    )

    # Stats duration histograms
    stats_request_duration_seconds = Histogram(
        "stats_request_duration_seconds",
        "Duration of statistics API requests in seconds",
        ["from_dialect", "to_dialect"],
        buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
        registry=registry,
    )

    stats_query_size_bytes = Histogram(
        "stats_query_size_bytes",
        "Size of input queries in statistics API in bytes",
        ["from_dialect", "to_dialect"],
        buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
        registry=registry,
    )

    stats_processing_step_duration = Histogram(
        "stats_processing_step_duration",
        "Duration of individual processing steps in statistics API",
        ["step_name", "from_dialect", "to_dialect"],
        buckets=[0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
        registry=registry,
    )
else:
    # Set all metrics to None if Prometheus is unavailable
    total_queries = None
    total_errors = None
    total_time_by_query = None
    total_time_by_process = None
    query_size_bytes = None
    stats_total_requests = None
    stats_total_errors = None
    stats_function_counts = None
    stats_executable_queries = None
    stats_request_duration_seconds = None
    stats_query_size_bytes = None
    stats_processing_step_duration = None


# ==================== Helper Functions ====================

def record_query_size(from_dialect: str, to_dialect: str, size_bytes: int):
    """Record the size of an input query."""
    if query_size_bytes:
        try:
            query_size_bytes.labels(from_dialect=from_dialect, to_dialect=to_dialect).observe(size_bytes)
        except Exception as e:
            logger.debug(f"Failed to record query size: {e}")


def record_query_duration(from_dialect: str, to_dialect: str, duration_seconds: float):
    """Record the total duration of a query conversion."""
    if total_time_by_query:
        try:
            total_time_by_query.labels(from_dialect=from_dialect, to_dialect=to_dialect).observe(duration_seconds)
        except Exception as e:
            logger.debug(f"Failed to record query duration: {e}")


def record_process_duration(step_name: str, from_dialect: str, to_dialect: str, duration_seconds: float):
    """Record the duration of a specific processing step."""
    if total_time_by_process:
        try:
            total_time_by_process.labels(
                step_name=step_name, from_dialect=from_dialect, to_dialect=to_dialect
            ).observe(duration_seconds)
        except Exception as e:
            logger.debug(f"Failed to record process duration: {e}")


def record_query_success(from_dialect: str, to_dialect: str):
    """Record a successful query conversion."""
    if total_queries:
        try:
            total_queries.labels(from_dialect=from_dialect, to_dialect=to_dialect, status="success").inc()
        except Exception as e:
            logger.debug(f"Failed to record query success: {e}")


def record_query_error(from_dialect: str, to_dialect: str, error_type: str):
    """Record a query conversion error."""
    if total_queries and total_errors:
        try:
            total_queries.labels(from_dialect=from_dialect, to_dialect=to_dialect, status="error").inc()
            total_errors.labels(from_dialect=from_dialect, to_dialect=to_dialect, error_type=error_type).inc()
        except Exception as e:
            logger.debug(f"Failed to record query error: {e}")


def get_metrics_response():
    """
    Generate Prometheus metrics response for multiprocess mode.

    Returns:
        tuple: (content, media_type) suitable for FastAPI Response
    """
    if not PROMETHEUS_AVAILABLE:
        return b"Prometheus not available", "text/plain"

    try:
        registry_for_metrics = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry_for_metrics)
        return generate_latest(registry_for_metrics), CONTENT_TYPE_LATEST
    except Exception as e:
        logger.error(f"Failed to generate metrics response: {e}")
        return b"Error generating metrics", "text/plain"


# ==================== Stats API Helper Functions ====================

def record_stats_request(from_dialect: str, to_dialect: str, status: str):
    """Record a statistics API request with its status."""
    if stats_total_requests:
        try:
            stats_total_requests.labels(from_dialect=from_dialect, to_dialect=to_dialect, status=status).inc()
        except Exception as e:
            logger.debug(f"Failed to record stats request: {e}")


def record_stats_duration(from_dialect: str, to_dialect: str, duration_seconds: float):
    """Record the total duration of a statistics API request."""
    if stats_request_duration_seconds:
        try:
            stats_request_duration_seconds.labels(from_dialect=from_dialect, to_dialect=to_dialect).observe(
                duration_seconds
            )
        except Exception as e:
            logger.debug(f"Failed to record stats duration: {e}")


def record_stats_function_count(from_dialect: str, to_dialect: str, function_type: str, count: int):
    """Record the count of functions by type (supported/unsupported/udf)."""
    if stats_function_counts:
        try:
            stats_function_counts.labels(
                from_dialect=from_dialect, to_dialect=to_dialect, function_type=function_type
            ).inc(count)
        except Exception as e:
            logger.debug(f"Failed to record stats function count: {e}")


def record_stats_executable_status(from_dialect: str, to_dialect: str, executable: str):
    """Record whether a query is executable or not."""
    if stats_executable_queries:
        try:
            stats_executable_queries.labels(from_dialect=from_dialect, to_dialect=to_dialect, executable=executable).inc()
        except Exception as e:
            logger.debug(f"Failed to record stats executable status: {e}")


def record_stats_error(from_dialect: str, to_dialect: str, error_type: str):
    """Record a statistics API error."""
    if stats_total_errors:
        try:
            stats_total_errors.labels(from_dialect=from_dialect, to_dialect=to_dialect, error_type=error_type).inc()
        except Exception as e:
            logger.debug(f"Failed to record stats error: {e}")


def record_stats_step_duration(step_name: str, from_dialect: str, to_dialect: str, duration_seconds: float):
    """Record the duration of a specific processing step in statistics API."""
    if stats_processing_step_duration:
        try:
            stats_processing_step_duration.labels(
                step_name=step_name, from_dialect=from_dialect, to_dialect=to_dialect
            ).observe(duration_seconds)
        except Exception as e:
            logger.debug(f"Failed to record stats step duration: {e}")


def record_stats_query_size(from_dialect: str, to_dialect: str, size_bytes: int):
    """Record the size of an input query in statistics API."""
    if stats_query_size_bytes:
        try:
            stats_query_size_bytes.labels(from_dialect=from_dialect, to_dialect=to_dialect).observe(size_bytes)
        except Exception as e:
            logger.debug(f"Failed to record stats query size: {e}")
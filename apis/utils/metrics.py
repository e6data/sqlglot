"""
Prometheus metrics module for converter API.

This module provides metrics collection and reporting functionality
for monitoring SQL conversion performance and errors.
"""

import os
from prometheus_client import (
    Counter,
    Histogram,
    CollectorRegistry,
    multiprocess,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ==================== Prometheus Setup ====================
PROMETHEUS_MULTIPROC_DIR = os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc_dir")
if not os.path.exists(PROMETHEUS_MULTIPROC_DIR):
    os.makedirs(PROMETHEUS_MULTIPROC_DIR, exist_ok=True)
os.environ["PROMETHEUS_MULTIPROC_DIR"] = PROMETHEUS_MULTIPROC_DIR

# Create a custom registry for multiprocess mode
registry = CollectorRegistry()

# ==================== Metric Definitions ====================

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


# ==================== Helper Functions ====================

def record_query_size(from_dialect: str, to_dialect: str, size_bytes: int):
    """Record the size of an input query."""
    query_size_bytes.labels(from_dialect=from_dialect, to_dialect=to_dialect).observe(size_bytes)


def record_query_duration(from_dialect: str, to_dialect: str, duration_seconds: float):
    """Record the total duration of a query conversion."""
    total_time_by_query.labels(from_dialect=from_dialect, to_dialect=to_dialect).observe(duration_seconds)


def record_process_duration(step_name: str, from_dialect: str, to_dialect: str, duration_seconds: float):
    """Record the duration of a specific processing step."""
    total_time_by_process.labels(
        step_name=step_name, from_dialect=from_dialect, to_dialect=to_dialect
    ).observe(duration_seconds)


def record_query_success(from_dialect: str, to_dialect: str):
    """Record a successful query conversion."""
    total_queries.labels(from_dialect=from_dialect, to_dialect=to_dialect, status="success").inc()


def record_query_error(from_dialect: str, to_dialect: str, error_type: str):
    """Record a query conversion error."""
    total_queries.labels(from_dialect=from_dialect, to_dialect=to_dialect, status="error").inc()
    total_errors.labels(from_dialect=from_dialect, to_dialect=to_dialect, error_type=error_type).inc()


def get_metrics_response():
    """
    Generate Prometheus metrics response for multiprocess mode.

    Returns:
        tuple: (content, media_type) suitable for FastAPI Response
    """
    registry_for_metrics = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry_for_metrics)
    return generate_latest(registry_for_metrics), CONTENT_TYPE_LATEST
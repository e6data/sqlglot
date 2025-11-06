"""
Deployment-level configuration for the SQLGlot transpiler service.

This module defines configuration that is set once at deployment time via
environment variables and remains constant throughout the application lifecycle.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache
from typing import Literal


class TranspilerConfig(BaseSettings):
    """
    Deployment-level configuration loaded from environment variables.

    All configuration fields can be set via environment variables with the E6_ prefix.
    Example: E6_UVICORN_WORKERS=4
    """

    # Server Configuration
    uvicorn_workers: int = Field(
        default=1,
        ge=1,
        le=32,
        description="Number of Uvicorn worker processes"
    )
    uvicorn_host: str = Field(
        default="0.0.0.0",
        description="Host to bind the server to"
    )
    uvicorn_port: int = Field(
        default=8100,
        ge=1,
        le=65535,
        description="Port to bind the server to"
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level for the application"
    )

    # API Configuration
    max_query_length: int = Field(
        default=1000000,
        ge=1,
        description="Maximum allowed query length in characters"
    )
    request_timeout: int = Field(
        default=120,
        ge=1,
        description="Request timeout in seconds"
    )

    # Transpilation Defaults
    default_target_dialect: str = Field(
        default="e6",
        description="Default target dialect for transpilation"
    )
    default_pretty_print: bool = Field(
        default=True,
        description="Default setting for pretty-printing SQL output"
    )
    default_enable_table_alias_qualification: bool = Field(
        default=False,
        description="Default setting for table alias qualification"
    )
    default_use_two_phase_qualification: bool = Field(
        default=False,
        description="Default setting for two-phase qualification scheme"
    )
    default_skip_e6_transpilation: bool = Field(
        default=False,
        description="Default setting for skipping E6 transpilation"
    )
    default_normalize_ascii: bool = Field(
        default=False,
        description="Default setting for ASCII normalization in transpilation"
    )
    default_error_level: Literal["IGNORE", "WARN", "RAISE"] = Field(
        default="RAISE",
        description="Default error handling level for SQL parsing"
    )

    # SQLGlot Dialect Configuration
    identifier_quote_char: str = Field(
        default='"',
        description="Character used to quote identifiers (table/column names)"
    )
    normalization_strategy: Literal["LOWERCASE", "UPPERCASE", "CASE_SENSITIVE", "CASE_INSENSITIVE"] = Field(
        default="LOWERCASE",
        description="Strategy for normalizing SQL keywords"
    )
    index_offset: int = Field(
        default=1,
        ge=0,
        le=1,
        description="Array indexing offset (0-based or 1-based)"
    )
    preserve_original_names: bool = Field(
        default=True,
        description="Preserve original identifier case sensitivity"
    )
    enable_identifier_quoting: bool = Field(
        default=True,
        description="Enable automatic quoting of identifiers with quote_identifiers()"
    )

    # Metrics Configuration
    enable_metrics: bool = Field(
        default=True,
        description="Enable Prometheus metrics collection"
    )
    metrics_endpoint: str = Field(
        default="/metrics",
        description="Path for Prometheus metrics endpoint"
    )
    metrics_track_phase_timings: bool = Field(
        default=True,
        description="Track detailed phase-level timing metrics for transpilation and analysis"
    )
    metrics_histogram_buckets: str = Field(
        default="0.01,0.05,0.1,0.5,1.0,2.5,5.0,10.0,30.0,60.0,120.0",
        description="Comma-separated histogram bucket values (in seconds) for duration metrics"
    )

    model_config = {
        "env_file": ".env",
        "env_prefix": "E6_",
        "frozen": True,
    }


@lru_cache()
def get_transpiler_config() -> TranspilerConfig:
    """
    Get the singleton instance of TranspilerConfig.

    Uses @lru_cache() to ensure the config is loaded only once and reused
    across all requests. This is the recommended pattern for Pydantic Settings.

    Returns:
        TranspilerConfig: The deployment configuration
    """
    return TranspilerConfig()

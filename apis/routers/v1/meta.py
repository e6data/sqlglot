from fastapi import APIRouter
from apis.models.responses import HealthResponse, DialectsResponse, DialectInfo, ConfigResponse, ConfigFieldInfo
from apis.utils.helpers import load_supported_functions
from apis.config import get_transpiler_config
import sqlglot

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint to verify API is running.
    """
    return HealthResponse(status="healthy", version="1.0.0")


@router.get("/dialects", response_model=DialectsResponse)
async def get_dialects():
    """
    Get list of all supported SQL dialects with metadata.

    Returns information about each dialect including:
    - Dialect name
    - Number of supported functions
    - Alternative names/aliases
    """
    # Get all available dialects from sqlglot
    dialect_names = [
        "bigquery",
        "clickhouse",
        "databricks",
        "drill",
        "duckdb",
        "e6",
        "hive",
        "mysql",
        "oracle",
        "postgres",
        "presto",
        "redshift",
        "snowflake",
        "spark",
        "sqlite",
        "teradata",
        "trino",
        "tsql",
    ]

    dialects = []
    for dialect_name in dialect_names:
        try:
            # Load supported functions for this dialect
            supported_funcs = load_supported_functions(dialect_name)
            function_count = len(supported_funcs) if supported_funcs else 0

            # Add dialect info
            dialects.append(
                DialectInfo(
                    name=dialect_name,
                    supported_functions_count=function_count,
                    aliases=[],
                )
            )
        except Exception:
            # Skip dialects that fail to load
            continue

    return DialectsResponse(dialects=dialects)


@router.get("/config", response_model=ConfigResponse)
async def get_config():
    """
    Get current deployment configuration (read-only).

    Returns the deployment-level configuration including server settings,
    API settings, and transpilation defaults. These values are set via
    environment variables and cannot be changed at runtime.
    """
    config = get_transpiler_config()

    # Server configuration
    server_fields = [
        ConfigFieldInfo(
            name="Uvicorn Workers",
            value=config.uvicorn_workers,
            description="Number of Uvicorn worker processes handling requests",
            type="integer"
        ),
        ConfigFieldInfo(
            name="Host",
            value=config.uvicorn_host,
            description="Host address the server is bound to",
            type="string"
        ),
        ConfigFieldInfo(
            name="Port",
            value=config.uvicorn_port,
            description="Port number the server is listening on",
            type="integer"
        ),
        ConfigFieldInfo(
            name="Log Level",
            value=config.log_level,
            description="Logging verbosity level for application logs",
            type="string"
        ),
    ]

    # API configuration
    api_fields = [
        ConfigFieldInfo(
            name="Max Query Length",
            value=config.max_query_length,
            description="Maximum allowed query length in characters",
            type="integer"
        ),
        ConfigFieldInfo(
            name="Request Timeout",
            value=config.request_timeout,
            description="Maximum time in seconds for a request to complete",
            type="integer"
        ),
    ]

    # Transpilation defaults
    transpilation_fields = [
        ConfigFieldInfo(
            name="Default Target Dialect",
            value=config.default_target_dialect,
            description="Default SQL dialect to transpile to when not specified in request",
            type="string"
        ),
        ConfigFieldInfo(
            name="Default Pretty Print",
            value=config.default_pretty_print,
            description="Default setting for formatting SQL output with proper indentation",
            type="boolean"
        ),
        ConfigFieldInfo(
            name="Default Table Alias Qualification",
            value=config.default_enable_table_alias_qualification,
            description="Default setting for adding table aliases to column references (e.g., users.id instead of id)",
            type="boolean"
        ),
        ConfigFieldInfo(
            name="Default Two-Phase Qualification",
            value=config.default_use_two_phase_qualification,
            description="Default setting for using two-phase qualification scheme for catalog.schema transformations",
            type="boolean"
        ),
        ConfigFieldInfo(
            name="Default Skip E6 Transpilation",
            value=config.default_skip_e6_transpilation,
            description="Default setting for skipping full E6 transpilation (only catalog.schema transformation)",
            type="boolean"
        ),
    ]

    return ConfigResponse(
        server=server_fields,
        api=api_fields,
        transpilation_defaults=transpilation_fields
    )

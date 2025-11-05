from fastapi import APIRouter
from apis.models.responses import HealthResponse, DialectsResponse, DialectInfo
from apis.utils.helpers import load_supported_functions
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

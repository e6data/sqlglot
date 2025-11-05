from fastapi import APIRouter, HTTPException
import sqlglot
import logging
from datetime import datetime
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot import parse_one

from apis.models.requests import TranspileRequest, AnalyzeRequest
from apis.models.responses import (
    TranspileResponse,
    AnalyzeResponse,
    FunctionAnalysis,
    QueryMetadata,
)
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

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/transpile", response_model=TranspileResponse)
async def transpile_inline(request: TranspileRequest):
    """
    Transpile a single SQL query from source dialect to target dialect.

    Returns the transpiled query without additional analysis metadata.
    """
    timestamp = datetime.now()

    try:
        logger.info(
            f"{request.query_id} AT {timestamp.isoformat()} FROM {request.source_dialect.upper()} — Starting transpilation"
        )

        # Normalize and clean query
        query = normalize_unicode_spaces(request.query)
        query, comment = strip_comment(query, "condenast")

        if not query.strip():
            raise HTTPException(status_code=400, detail="Empty query provided")

        # Set E6 dialect flags
        from sqlglot.dialects.e6 import E6
        original_qualification_flag = E6.ENABLE_TABLE_ALIAS_QUALIFICATION
        E6.ENABLE_TABLE_ALIAS_QUALIFICATION = request.options.table_alias_qualification

        try:
            # Parse query
            tree = sqlglot.parse_one(query, read=request.source_dialect, error_level=None)

            # Handle two-phase qualification if enabled
            if request.options.use_two_phase_qualification_scheme:
                if request.options.skip_e6_transpilation:
                    transformed_query = transform_catalog_schema_only(query, request.source_dialect)
                    transformed_query = add_comment_to_query(transformed_query, comment)
                    return TranspileResponse(
                        transpiled_query=transformed_query,
                        source_dialect=request.source_dialect,
                        target_dialect=request.target_dialect,
                        query_id=request.query_id,
                    )
                tree = transform_table_part(tree)

            # Qualify identifiers
            tree2 = quote_identifiers(tree, dialect=request.target_dialect)

            # Ensure SELECT FROM VALUES
            values_ensured_ast = ensure_select_from_values(tree2)

            # Set CTE names case-sensitively
            cte_names_checked_ast = set_cte_names_case_sensitively(values_ensured_ast)

            # Generate SQL
            transpiled_query = cte_names_checked_ast.sql(
                dialect=request.target_dialect,
                from_dialect=request.source_dialect,
                pretty=request.options.pretty_print,
            )

            # Post-process
            transpiled_query = replace_struct_in_query(transpiled_query)
            transpiled_query = add_comment_to_query(transpiled_query, comment)

            logger.info(
                f"{request.query_id} — Transpilation completed in {(datetime.now() - timestamp).total_seconds():.3f}s"
            )

            return TranspileResponse(
                transpiled_query=transpiled_query,
                source_dialect=request.source_dialect,
                target_dialect=request.target_dialect,
                query_id=request.query_id,
            )

        finally:
            # Restore original flag
            E6.ENABLE_TABLE_ALIAS_QUALIFICATION = original_qualification_flag

    except Exception as e:
        logger.error(
            f"{request.query_id} — Transpilation failed: {str(e)}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze", response_model=AnalyzeResponse)
async def analyze_inline(request: AnalyzeRequest):
    """
    Analyze and transpile a single SQL query.

    Returns transpiled query along with detailed metadata including:
    - Function compatibility (supported/unsupported)
    - Tables, joins, CTEs, subqueries
    - Whether query is executable on target dialect
    """
    timestamp = datetime.now()

    try:
        logger.info(
            f"{request.query_id} AT {timestamp.isoformat()} FROM {request.source_dialect.upper()} — Starting analysis"
        )

        # Normalize and clean query
        query = normalize_unicode_spaces(request.query)
        query, comment = strip_comment(query, "condenast")

        if not query.strip():
            raise HTTPException(status_code=400, detail="Empty query provided")

        # Load supported functions
        supported_functions_in_target = load_supported_functions(request.target_dialect)
        supported_functions_in_source = load_supported_functions(request.source_dialect)

        # Function extraction patterns
        functions_as_keywords = ["LIKE", "ILIKE", "RLIKE", "AT TIME ZONE", "||", "DISTINCT", "QUALIFY"]
        exclusion_list = ["AS", "AND", "THEN", "OR", "ELSE", "WHEN", "WHERE", "FROM", "JOIN", "OVER", "ON", "ALL", "NOT", "BETWEEN", "UNION", "SELECT", "BY", "GROUP", "EXCEPT", "SETS"]

        function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
        keyword_pattern = r"\b(?:" + "|".join([f"\\{func}" for func in functions_as_keywords]) + r")\b"

        # Extract and categorize functions
        all_functions = extract_functions_from_query(query, function_pattern, keyword_pattern, exclusion_list)
        supported, unsupported = categorize_functions(all_functions, supported_functions_in_target, functions_as_keywords)
        udf_list, unsupported = extract_udfs(unsupported, supported_functions_in_source)

        # Parse and analyze query structure
        original_ast = parse_one(query, read=request.source_dialect)
        tables_list = extract_db_and_Table_names(original_ast)
        joins_list = extract_joins_from_query(original_ast)
        cte_subquery_list = extract_cte_n_subquery_list(original_ast)

        # Check for unsupported functionality
        supported, unsupported = unsupported_functionality_identifiers(original_ast, unsupported, supported)

        # Transpile query
        values_ensured_ast = ensure_select_from_values(original_ast)
        cte_names_ast = set_cte_names_case_sensitively(values_ensured_ast)
        query = cte_names_ast.sql(request.source_dialect)

        tree = sqlglot.parse_one(query, read=request.source_dialect, error_level=None)
        tree2 = quote_identifiers(tree, dialect=request.target_dialect)

        transpiled_query = tree2.sql(
            dialect=request.target_dialect,
            from_dialect=request.source_dialect,
            pretty=request.options.pretty_print,
        )

        transpiled_query = replace_struct_in_query(transpiled_query)
        transpiled_query = add_comment_to_query(transpiled_query, comment)

        # Analyze transpiled query
        transpiled_ast = parse_one(transpiled_query, read=request.target_dialect)
        all_functions_transpiled = extract_functions_from_query(transpiled_query, function_pattern, keyword_pattern, exclusion_list)
        supported_transpiled, unsupported_transpiled = categorize_functions(all_functions_transpiled, supported_functions_in_target, functions_as_keywords)
        supported_transpiled, unsupported_transpiled = unsupported_functionality_identifiers(transpiled_ast, unsupported_transpiled, supported_transpiled)

        # Determine if executable
        executable = len(unsupported_transpiled) == 0

        logger.info(
            f"{request.query_id} — Analysis completed in {(datetime.now() - timestamp).total_seconds():.3f}s"
        )

        return AnalyzeResponse(
            transpiled_query=transpiled_query,
            source_dialect=request.source_dialect,
            target_dialect=request.target_dialect,
            query_id=request.query_id,
            executable=executable,
            functions=FunctionAnalysis(
                supported=list(supported_transpiled),
                unsupported=list(unsupported_transpiled),
            ),
            metadata=QueryMetadata(
                tables=list(tables_list),
                joins=joins_list,
                ctes=cte_subquery_list.get("ctes", []),
                subqueries=cte_subquery_list.get("subqueries", []),
                udfs=list(udf_list),
            ),
        )

    except Exception as e:
        logger.error(
            f"{request.query_id} — Analysis failed: {str(e)}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=str(e))

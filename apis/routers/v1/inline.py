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
    TimingInfo,
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
from apis.context import set_per_request_config, PerRequestConfig

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

        # Set per-request configuration
        per_request_config = PerRequestConfig(
            enable_table_alias_qualification=request.options.table_alias_qualification,
            use_two_phase_qualification_scheme=request.options.use_two_phase_qualification_scheme,
            skip_e6_transpilation=request.options.skip_e6_transpilation,
            pretty_print=request.options.pretty_print,
        )
        set_per_request_config(per_request_config)

        # Normalize and clean query
        query = normalize_unicode_spaces(request.query)
        query, comment = strip_comment(query, "condenast")

        if not query.strip():
            raise HTTPException(status_code=400, detail="Empty query provided")

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
    start_time = datetime.now()
    timings = {}

    try:
        logger.info(
            f"{request.query_id} AT {start_time.isoformat()} FROM {request.source_dialect.upper()} — Starting analysis"
        )

        # Set per-request configuration
        per_request_config = PerRequestConfig(
            enable_table_alias_qualification=request.options.table_alias_qualification,
            use_two_phase_qualification_scheme=request.options.use_two_phase_qualification_scheme,
            skip_e6_transpilation=request.options.skip_e6_transpilation,
            pretty_print=request.options.pretty_print,
        )
        set_per_request_config(per_request_config)

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

        # Phase 1: Parsing
        phase_start = datetime.now()
        original_ast = parse_one(query, read=request.source_dialect)
        timings['parsing_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        # Phase 2: Function Analysis
        phase_start = datetime.now()
        all_functions = extract_functions_from_query(query, function_pattern, keyword_pattern, exclusion_list)
        supported, unsupported = categorize_functions(all_functions, supported_functions_in_target, functions_as_keywords)
        udf_list, unsupported = extract_udfs(unsupported, supported_functions_in_source)
        supported, unsupported = unsupported_functionality_identifiers(original_ast, unsupported, supported)
        timings['function_analysis_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        # Phase 3: Metadata Extraction
        phase_start = datetime.now()
        tables_list = extract_db_and_Table_names(original_ast)
        joins_list = extract_joins_from_query(original_ast)
        # extract_cte_n_subquery_list returns [cte_list, values_list, subquery_list]
        cte_values_subquery_result = extract_cte_n_subquery_list(original_ast)
        cte_list = cte_values_subquery_result[0] if len(cte_values_subquery_result) > 0 else []
        values_list = cte_values_subquery_result[1] if len(cte_values_subquery_result) > 1 else []
        subquery_list = cte_values_subquery_result[2] if len(cte_values_subquery_result) > 2 else []

        # Extract schemas from tables
        schemas_set = set()
        for table in tables_list:
            if '.' in table:
                schema = table.split('.')[0]
                schemas_set.add(schema)
        schemas_list = list(schemas_set)
        timings['metadata_extraction_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        # Phase 4: Transpilation
        phase_start = datetime.now()
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
        timings['transpilation_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        # Phase 5: Post-Transpilation Analysis
        phase_start = datetime.now()
        transpiled_ast = parse_one(transpiled_query, read=request.target_dialect)
        all_functions_transpiled = extract_functions_from_query(transpiled_query, function_pattern, keyword_pattern, exclusion_list)
        supported_transpiled, unsupported_transpiled = categorize_functions(all_functions_transpiled, supported_functions_in_target, functions_as_keywords)
        supported_transpiled, unsupported_transpiled = unsupported_functionality_identifiers(transpiled_ast, unsupported_transpiled, supported_transpiled)
        timings['post_analysis_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        # Determine if executable
        executable = len(unsupported_transpiled) == 0

        # Generate ASTs
        source_ast_dict = original_ast.dump()
        transpiled_ast_dict = transpiled_ast.dump()

        # Calculate total time
        total_time = (datetime.now() - start_time).total_seconds() * 1000
        timings['total_ms'] = total_time

        logger.info(
            f"{request.query_id} — Analysis completed in {total_time:.2f}ms"
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
                ctes=cte_list,
                subqueries=subquery_list,
                udfs=list(udf_list),
                schemas=schemas_list,
            ),
            source_ast=source_ast_dict,
            transpiled_ast=transpiled_ast_dict,
            timing=TimingInfo(**timings),
        )

    except Exception as e:
        logger.error(
            f"{request.query_id} — Analysis failed: {str(e)}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=str(e))

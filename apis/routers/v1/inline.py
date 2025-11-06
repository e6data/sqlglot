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
from apis.config import get_transpiler_config

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
            "transpilation_started",
            extra={
                "query_id": request.query_id,
                "timestamp": timestamp.isoformat(),
                "source_dialect": request.source_dialect,
                "target_dialect": request.target_dialect,
            },
        )

        # Set per-request configuration
        per_request_config = PerRequestConfig(
            enable_table_alias_qualification=request.options.table_alias_qualification,
            use_two_phase_qualification_scheme=request.options.use_two_phase_qualification_scheme,
            skip_e6_transpilation=request.options.skip_e6_transpilation,
            pretty_print=request.options.pretty_print,
        )
        set_per_request_config(per_request_config)

        # Get deployment config for ASCII normalization
        config = get_transpiler_config()

        # Normalize and clean query
        logger.debug("phase_normalization_started", extra={"query_id": request.query_id})
        query = request.query
        if config.default_normalize_ascii:
            query = normalize_unicode_spaces(query)
        query, comment = strip_comment(query, "condenast")
        logger.debug("phase_normalization_completed", extra={"query_id": request.query_id})

        if not query.strip():
            raise HTTPException(status_code=400, detail="Empty query provided")

        # Parse query
        logger.debug("phase_parsing_started", extra={"query_id": request.query_id})
        tree = sqlglot.parse_one(query, read=request.source_dialect, error_level=None)
        logger.debug("phase_parsing_completed", extra={"query_id": request.query_id})

        # Handle two-phase qualification if enabled
        if request.options.use_two_phase_qualification_scheme:
            logger.debug("phase_two_phase_qualification_started", extra={"query_id": request.query_id})
            if request.options.skip_e6_transpilation:
                transformed_query = transform_catalog_schema_only(query, request.source_dialect)
                transformed_query = add_comment_to_query(transformed_query, comment)
                logger.debug("phase_two_phase_qualification_completed", extra={"query_id": request.query_id, "skipped_transpilation": True})
                return TranspileResponse(
                    transpiled_query=transformed_query,
                    source_dialect=request.source_dialect,
                    target_dialect=request.target_dialect,
                    query_id=request.query_id,
                )
            tree = transform_table_part(tree)
            logger.debug("phase_two_phase_qualification_completed", extra={"query_id": request.query_id})

        # Qualify identifiers (if enabled in system config)
        logger.debug("phase_identifier_qualification_started", extra={"query_id": request.query_id})
        if config.enable_identifier_quoting:
            tree2 = quote_identifiers(tree, dialect=request.target_dialect)
        else:
            tree2 = tree
        logger.debug("phase_identifier_qualification_completed", extra={"query_id": request.query_id})

        # Ensure SELECT FROM VALUES
        logger.debug("phase_ast_preprocessing_started", extra={"query_id": request.query_id})
        values_ensured_ast = ensure_select_from_values(tree2)

        # Set CTE names case-sensitively
        cte_names_checked_ast = set_cte_names_case_sensitively(values_ensured_ast)
        logger.debug("phase_ast_preprocessing_completed", extra={"query_id": request.query_id})

        # Generate SQL
        logger.debug("phase_sql_generation_started", extra={"query_id": request.query_id})
        transpiled_query = cte_names_checked_ast.sql(
            dialect=request.target_dialect,
            from_dialect=request.source_dialect,
            pretty=request.options.pretty_print,
        )
        logger.debug("phase_sql_generation_completed", extra={"query_id": request.query_id})

        # Post-process
        logger.debug("phase_post_processing_started", extra={"query_id": request.query_id})
        transpiled_query = replace_struct_in_query(transpiled_query)
        transpiled_query = add_comment_to_query(transpiled_query, comment)
        logger.debug("phase_post_processing_completed", extra={"query_id": request.query_id})

        duration_s = (datetime.now() - timestamp).total_seconds()
        logger.info(
            "transpilation_completed",
            extra={
                "query_id": request.query_id,
                "duration_s": duration_s,
                "source_dialect": request.source_dialect,
                "target_dialect": request.target_dialect,
            },
        )

        return TranspileResponse(
            transpiled_query=transpiled_query,
            source_dialect=request.source_dialect,
            target_dialect=request.target_dialect,
            query_id=request.query_id,
        )

    except Exception as e:
        logger.error(
            "transpilation_failed",
            extra={
                "query_id": request.query_id,
                "error": str(e),
                "source_dialect": request.source_dialect,
                "target_dialect": request.target_dialect,
            },
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
            "analysis_started",
            extra={
                "query_id": request.query_id,
                "timestamp": start_time.isoformat(),
                "source_dialect": request.source_dialect,
                "target_dialect": request.target_dialect,
            },
        )

        # Set per-request configuration
        per_request_config = PerRequestConfig(
            enable_table_alias_qualification=request.options.table_alias_qualification,
            use_two_phase_qualification_scheme=request.options.use_two_phase_qualification_scheme,
            skip_e6_transpilation=request.options.skip_e6_transpilation,
            pretty_print=request.options.pretty_print,
        )
        set_per_request_config(per_request_config)

        # Get deployment config for ASCII normalization
        config = get_transpiler_config()

        # Preprocessing: Normalization
        logger.debug("phase_normalization_started", extra={"query_id": request.query_id})
        phase_start = datetime.now()
        query = request.query
        if config.default_normalize_ascii:
            query = normalize_unicode_spaces(query)
        query, comment = strip_comment(query, "condenast")
        timings['normalization_ms'] = (datetime.now() - phase_start).total_seconds() * 1000
        logger.debug("phase_normalization_completed", extra={"query_id": request.query_id, "duration_ms": timings['normalization_ms']})

        if not query.strip():
            raise HTTPException(status_code=400, detail="Empty query provided")

        # Preprocessing: Config Loading
        logger.debug("phase_config_loading_started", extra={"query_id": request.query_id})
        phase_start = datetime.now()
        supported_functions_in_target = load_supported_functions(request.target_dialect)
        supported_functions_in_source = load_supported_functions(request.source_dialect)
        timings['config_loading_ms'] = (datetime.now() - phase_start).total_seconds() * 1000
        logger.debug("phase_config_loading_completed", extra={"query_id": request.query_id, "duration_ms": timings['config_loading_ms']})

        # Function extraction patterns
        functions_as_keywords = ["LIKE", "ILIKE", "RLIKE", "AT TIME ZONE", "||", "DISTINCT", "QUALIFY"]
        exclusion_list = ["AS", "AND", "THEN", "OR", "ELSE", "WHEN", "WHERE", "FROM", "JOIN", "OVER", "ON", "ALL", "NOT", "BETWEEN", "UNION", "SELECT", "BY", "GROUP", "EXCEPT", "SETS"]

        function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
        keyword_pattern = r"\b(?:" + "|".join([f"\\{func}" for func in functions_as_keywords]) + r")\b"

        # Phase 1: Parsing
        logger.debug("phase_parsing_started", extra={"query_id": request.query_id})
        phase_start = datetime.now()
        original_ast = parse_one(query, read=request.source_dialect)
        timings['parsing_ms'] = (datetime.now() - phase_start).total_seconds() * 1000
        logger.debug("phase_parsing_completed", extra={"query_id": request.query_id, "duration_ms": timings['parsing_ms']})

        # Phase 2: Function Analysis (detailed)
        logger.debug("phase_function_analysis_started", extra={"query_id": request.query_id})
        function_analysis_start = datetime.now()

        phase_start = datetime.now()
        all_functions = extract_functions_from_query(query, function_pattern, keyword_pattern, exclusion_list)
        timings['function_extraction_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        supported, unsupported = categorize_functions(all_functions, supported_functions_in_target, functions_as_keywords)
        timings['function_categorization_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        udf_list, unsupported = extract_udfs(unsupported, supported_functions_in_source)
        timings['udf_extraction_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        supported, unsupported = unsupported_functionality_identifiers(original_ast, unsupported, supported)
        timings['unsupported_detection_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        timings['function_analysis_ms'] = (datetime.now() - function_analysis_start).total_seconds() * 1000
        logger.debug("phase_function_analysis_completed", extra={"query_id": request.query_id, "duration_ms": timings['function_analysis_ms']})

        # Phase 3: Metadata Extraction (detailed)
        logger.debug("phase_metadata_extraction_started", extra={"query_id": request.query_id})
        metadata_extraction_start = datetime.now()

        phase_start = datetime.now()
        tables_list = extract_db_and_Table_names(original_ast)
        timings['table_extraction_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        joins_list = extract_joins_from_query(original_ast)
        timings['join_extraction_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        # extract_cte_n_subquery_list returns [cte_list, values_list, subquery_list]
        phase_start = datetime.now()
        cte_values_subquery_result = extract_cte_n_subquery_list(original_ast)
        cte_list = cte_values_subquery_result[0] if len(cte_values_subquery_result) > 0 else []
        values_list = cte_values_subquery_result[1] if len(cte_values_subquery_result) > 1 else []
        subquery_list = cte_values_subquery_result[2] if len(cte_values_subquery_result) > 2 else []
        timings['cte_extraction_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        # Extract schemas from tables
        phase_start = datetime.now()
        schemas_set = set()
        for table in tables_list:
            if '.' in table:
                schema = table.split('.')[0]
                schemas_set.add(schema)
        schemas_list = list(schemas_set)
        timings['schema_extraction_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        timings['metadata_extraction_ms'] = (datetime.now() - metadata_extraction_start).total_seconds() * 1000
        logger.debug("phase_metadata_extraction_completed", extra={"query_id": request.query_id, "duration_ms": timings['metadata_extraction_ms']})

        # Phase 4: Transpilation (detailed)
        logger.debug("phase_transpilation_started", extra={"query_id": request.query_id})
        transpilation_start = datetime.now()

        phase_start = datetime.now()
        values_ensured_ast = ensure_select_from_values(original_ast)
        cte_names_ast = set_cte_names_case_sensitively(values_ensured_ast)
        query = cte_names_ast.sql(request.source_dialect)
        timings['ast_preprocessing_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        tree = sqlglot.parse_one(query, read=request.source_dialect, error_level=None)
        timings['transpilation_parsing_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        if config.enable_identifier_quoting:
            tree2 = quote_identifiers(tree, dialect=request.target_dialect)
        else:
            tree2 = tree
        timings['identifier_qualification_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        transpiled_query = tree2.sql(
            dialect=request.target_dialect,
            from_dialect=request.source_dialect,
            pretty=request.options.pretty_print,
        )
        timings['sql_generation_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        transpiled_query = replace_struct_in_query(transpiled_query)
        transpiled_query = add_comment_to_query(transpiled_query, comment)
        timings['post_processing_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        timings['transpilation_ms'] = (datetime.now() - transpilation_start).total_seconds() * 1000
        logger.debug("phase_transpilation_completed", extra={"query_id": request.query_id, "duration_ms": timings['transpilation_ms']})

        # Phase 5: Post-Transpilation Analysis (detailed)
        logger.debug("phase_post_transpilation_analysis_started", extra={"query_id": request.query_id})
        post_analysis_start = datetime.now()

        phase_start = datetime.now()
        transpiled_ast = parse_one(transpiled_query, read=request.target_dialect)
        timings['transpiled_parsing_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        all_functions_transpiled = extract_functions_from_query(transpiled_query, function_pattern, keyword_pattern, exclusion_list)
        timings['transpiled_function_extraction_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        phase_start = datetime.now()
        supported_transpiled, unsupported_transpiled = categorize_functions(all_functions_transpiled, supported_functions_in_target, functions_as_keywords)
        supported_transpiled, unsupported_transpiled = unsupported_functionality_identifiers(transpiled_ast, unsupported_transpiled, supported_transpiled)
        timings['transpiled_function_analysis_ms'] = (datetime.now() - phase_start).total_seconds() * 1000

        timings['post_analysis_ms'] = (datetime.now() - post_analysis_start).total_seconds() * 1000
        logger.debug("phase_post_transpilation_analysis_completed", extra={"query_id": request.query_id, "duration_ms": timings['post_analysis_ms']})

        # Determine if executable
        executable = len(unsupported_transpiled) == 0

        # Final: AST Serialization
        logger.debug("phase_ast_serialization_started", extra={"query_id": request.query_id})
        phase_start = datetime.now()
        source_ast_dict = original_ast.dump()
        transpiled_ast_dict = transpiled_ast.dump()
        timings['ast_serialization_ms'] = (datetime.now() - phase_start).total_seconds() * 1000
        logger.debug("phase_ast_serialization_completed", extra={"query_id": request.query_id, "duration_ms": timings['ast_serialization_ms']})

        # Calculate total time
        total_time = (datetime.now() - start_time).total_seconds() * 1000
        timings['total_ms'] = total_time

        logger.info(
            "analysis_completed",
            extra={
                "query_id": request.query_id,
                "duration_ms": total_time,
                "source_dialect": request.source_dialect,
                "target_dialect": request.target_dialect,
                "executable": executable,
            },
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
            "analysis_failed",
            extra={
                "query_id": request.query_id,
                "error": str(e),
                "source_dialect": request.source_dialect,
                "target_dialect": request.target_dialect,
            },
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=str(e))

"""
Statistics Analysis Module
Provides query analysis functionality for automated processing
"""
import sys
import os
import re
import json
import logging
from typing import Dict, Any, Set, List

# Add parent directory to path to access converter_api modules
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

logger = logging.getLogger(__name__)


def analyze_sql_functions(query: str, from_sql: str, query_id: str, to_sql: str = "e6") -> Dict[str, Any]:
    """
    Analyze SQL query and extract supported/unsupported functions, transpile, and return comprehensive stats
    This is a simplified version of the converter_api statistics endpoint logic
    """
    try:
        # Import required modules
        from apis.utils.helpers import (
            strip_comment, normalize_unicode_spaces, extract_functions_from_query,
            categorize_functions, add_comment_to_query, replace_struct_in_query,
            ensure_select_from_values, extract_udfs, load_supported_functions,
            extract_db_and_Table_names, extract_joins_from_query,
            extract_cte_n_subquery_list, set_cte_names_case_sensitively,
            unsupported_functionality_identifiers
        )
        
        import sqlglot
        from sqlglot import parse_one
        from sqlglot.optimizer.qualify_columns import quote_identifiers
        
        # Configuration
        supported_functions_in_target = load_supported_functions(to_sql)
        functions_as_keywords = ["LIKE", "ILIKE", "RLIKE", "AT TIME ZONE", "||", "DISTINCT", "QUALIFY"]
        exclusion_list = ["AS", "AND", "THEN", "OR", "ELSE", "WHEN", "WHERE", "FROM", "JOIN", "OVER", "ON", 
                         "ALL", "NOT", "BETWEEN", "UNION", "SELECT", "BY", "GROUP", "EXCEPT", "SETS"]
        
        function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
        keyword_pattern = r"\b(?:" + "|".join([re.escape(func) for func in functions_as_keywords]) + r")\b"
        
        # Handle empty queries
        if not query or not query.strip():
            return {
                "query_id": query_id,
                "supported_functions": [],
                "unsupported_functions": [],
                "udf_list": [],
                "converted-query": "Query is empty or only contains comments.",
                "unsupported_functions_after_transpilation": [],
                "executable": "NO",
                "tables_list": [],
                "joins_list": [],
                "cte_values_subquery_list": [],
                "error": True
            }
        
        # Normalize and clean query
        query = normalize_unicode_spaces(query)
        query, comment = strip_comment(query, "condenast")
        
        # Extract functions from original query
        all_functions = extract_functions_from_query(query, function_pattern, keyword_pattern, exclusion_list)
        supported, unsupported = categorize_functions(all_functions, supported_functions_in_target, functions_as_keywords)
        
        # Parse and analyze query structure
        original_ast = parse_one(query, read=from_sql)
        tables_list = extract_db_and_Table_names(original_ast)
        
        # Extract UDFs
        from_dialect_functions = load_supported_functions(from_sql)
        udf_list, unsupported = extract_udfs(unsupported, from_dialect_functions, tables_list)
        
        # Default values
        executable = "YES"
        error_flag = False
        converted_query = ""
        unsupported_after_transpilation = []
        joins_list = []
        cte_values_subquery_list = []
        
        try:
            # Process original query
            supported, unsupported = unsupported_functionality_identifiers(original_ast, unsupported, supported)
            values_ensured_ast = ensure_select_from_values(original_ast)
            cte_names_equivalence_ast = set_cte_names_case_sensitively(values_ensured_ast)
            normalized_query = cte_names_equivalence_ast.sql(from_sql)
            
            # Transpile query
            tree = sqlglot.parse_one(normalized_query, read=from_sql, error_level=None)
            tree2 = quote_identifiers(tree, dialect=to_sql)
            converted_query = tree2.sql(dialect=to_sql, from_dialect=from_sql, pretty=True)
            
            # Post-process converted query
            converted_query = replace_struct_in_query(converted_query)
            converted_query = add_comment_to_query(converted_query, comment)
            
            # Analyze converted query for unsupported functions
            all_functions_converted = extract_functions_from_query(
                converted_query, function_pattern, keyword_pattern, exclusion_list
            )
            supported_converted, unsupported_converted = categorize_functions(
                all_functions_converted, supported_functions_in_target, functions_as_keywords
            )
            
            # Filter UDFs from converted query analysis
            _, unsupported_converted = extract_udfs(unsupported_converted, from_dialect_functions, tables_list)
            
            # Check converted query AST for unsupported functionality
            converted_ast = parse_one(converted_query, read=to_sql)
            _, unsupported_after_transpilation = unsupported_functionality_identifiers(
                converted_ast, unsupported_converted, supported_converted
            )
            
            # Extract structural information
            joins_list = extract_joins_from_query(original_ast)
            cte_values_subquery_list = extract_cte_n_subquery_list(original_ast)
            
            # Determine executability
            if unsupported_after_transpilation:
                executable = "NO"
                
        except Exception as e:
            logger.warning(f"Query {query_id} transpilation failed: {str(e)}")
            error_flag = True
            converted_query = f"Transpilation Error: {str(e)}"
            executable = "NO"
            unsupported_after_transpilation = []
            joins_list = []
            cte_values_subquery_list = []
        
        # Return comprehensive analysis
        return {
            "query_id": query_id,
            "supported_functions": list(supported),
            "unsupported_functions": list(set(unsupported)),
            "udf_list": list(set(udf_list)),
            "converted-query": converted_query,
            "unsupported_functions_after_transpilation": list(set(unsupported_after_transpilation)),
            "executable": executable,
            "tables_list": list(set(tables_list)),
            "joins_list": joins_list,
            "cte_values_subquery_list": cte_values_subquery_list,
            "error": error_flag
        }
        
    except Exception as e:
        logger.error(f"Error analyzing query {query_id}: {str(e)}")
        return {
            "query_id": query_id,
            "supported_functions": [],
            "unsupported_functions": [],
            "udf_list": [],
            "converted-query": f"Analysis Error: {str(e)}",
            "unsupported_functions_after_transpilation": [],
            "executable": "NO",
            "tables_list": [],
            "joins_list": [],
            "cte_values_subquery_list": [],
            "error": True
        }
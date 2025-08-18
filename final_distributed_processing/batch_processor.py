"""
Batch Processor Module
Implements hash-based modulo batch distribution for SQL query processing
"""
import pandas as pd
import hashlib
import logging
import time
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def hash_query(query_text: str) -> int:
    """
    Generate a consistent hash for a query string using SHA256
    
    Args:
        query_text: SQL query string
        
    Returns:
        Integer hash value
    """
    sha256_hash = hashlib.sha256(query_text.strip().encode()).hexdigest()
    return int(sha256_hash[:8], 16)


def calculate_optimal_batches(file_path: str, query_column: str, target_batch_size: int = 10000) -> Dict[str, Any]:
    """
    Pre-scan file to count unique queries and determine optimal batch count.
    Fast scan since we only need to count unique values.
    
    Args:
        file_path: Path to parquet file
        query_column: Column containing queries
        target_batch_size: Target number of unique queries per batch
        
    Returns:
        Dict with batch calculation results
    """
    start_time = time.time()
    
    try:
        # Fast scan - read only the query column
        df_queries = pd.read_parquet(file_path, columns=[query_column])
        
        # Count unique queries
        unique_count = df_queries[query_column].nunique()
        total_count = len(df_queries)
        
        scan_time = time.time() - start_time
        
        # Calculate optimal batches based on unique count
        num_batches = max(1, unique_count // target_batch_size)
        queries_per_batch = unique_count // num_batches if num_batches > 0 else unique_count
        
        logger.info(f"Pre-scan completed for {Path(file_path).name}:")
        logger.info(f"  Total rows: {total_count}")
        logger.info(f"  Unique queries: {unique_count}")
        logger.info(f"  Scan time: {scan_time:.2f}s")
        logger.info(f"  Optimal batches: {num_batches}")
        logger.info(f"  ~{queries_per_batch} unique queries per batch")
        
        return {
            "num_batches": num_batches,
            "unique_count": unique_count,
            "total_count": total_count,
            "scan_time_seconds": scan_time,
            "queries_per_batch": queries_per_batch,
            "file_path": file_path,
            "file_name": Path(file_path).name
        }
        
    except Exception as e:
        logger.error(f"Error pre-scanning {file_path}: {str(e)}")
        return {
            "error": str(e),
            "file_path": file_path,
            "file_name": Path(file_path).name
        }


def create_modulo_batch(
    file_path: str, 
    query_column: str,
    remainder: int, 
    total_batches: int,
    chunk_size: int = 50000
) -> Dict[str, str]:
    """
    Create a batch dictionary by filtering queries based on modulo assignment.
    Workers create batches on-the-fly without storing them.
    
    Args:
        file_path: Path to parquet file
        query_column: Column containing queries
        remainder: Modulo remainder for this batch (0 to total_batches-1)
        total_batches: Total number of batches
        chunk_size: Size of chunks for streaming large files
        
    Returns:
        Dictionary mapping query_hash -> query_text for queries in this batch
    """
    batch_dict = {}
    processed_rows = 0
    
    logger.info(f"Creating modulo batch {remainder}/{total_batches} from {Path(file_path).name}")
    
    try:
        # Read the entire parquet file (parquet doesn't support chunking like CSV)
        df = pd.read_parquet(file_path)
        
        for _, row in df.iterrows():
            query_text = str(row[query_column])
            
            if query_text and query_text.strip():
                # Calculate hash and check modulo condition
                query_hash = hash_query(query_text)
                
                if query_hash % total_batches == remainder:
                    # This query belongs to this batch
                    batch_dict[str(query_hash)] = query_text
            
            processed_rows += 1
            
            # Log progress for large files
            if processed_rows % 100000 == 0:
                logger.info(f"Processed {processed_rows} rows, found {len(batch_dict)} queries for batch {remainder}")
        
        logger.info(f"Modulo batch {remainder} created: {len(batch_dict)} unique queries from {processed_rows} total rows")
        
        return batch_dict
        
    except Exception as e:
        logger.error(f"Error creating modulo batch {remainder} from {file_path}: {str(e)}")
        raise


def process_batch_queries(
    batch_dict: Dict[str, str],
    from_dialect: str,
    to_dialect: str,
    batch_id: str
) -> List[Dict[str, Any]]:
    """
    Process all queries in a batch dictionary using the existing converter logic
    
    Args:
        batch_dict: Dictionary of query_hash -> query_text
        from_dialect: Source SQL dialect
        to_dialect: Target SQL dialect
        batch_id: Identifier for this batch
        
    Returns:
        List of processing results for each query
    """
    results = []
    processed_count = 0
    success_count = 0
    
    logger.info(f"Processing batch {batch_id}: {len(batch_dict)} unique queries")
    
    # Import converter functions locally to avoid circular imports
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    
    try:
        from apis.utils.helpers import (
            strip_comment, normalize_unicode_spaces, extract_functions_from_query,
            categorize_functions, add_comment_to_query, replace_struct_in_query,
            ensure_select_from_values, extract_udfs, load_supported_functions,
            extract_db_and_Table_names, extract_joins_from_query,
            extract_cte_n_subquery_list, set_cte_names_case_sensitively,
            unsupported_functionality_identifiers
        )
        from sqlglot import parse_one
        from sqlglot.optimizer.qualify_columns import quote_identifiers
        
        # Load supported functions for analysis
        supported_functions_in_target = load_supported_functions(to_dialect)
        from_dialect_functions = load_supported_functions(from_dialect)
        
        functions_as_keywords = [
            "LIKE", "ILIKE", "RLIKE", "AT TIME ZONE", "||", "DISTINCT", "QUALIFY"
        ]
        
        exclusion_list = [
            "AS", "AND", "THEN", "OR", "ELSE", "WHEN", "WHERE", "FROM", 
            "JOIN", "OVER", "ON", "ALL", "NOT", "BETWEEN", "UNION", 
            "SELECT", "BY", "GROUP", "EXCEPT", "SETS"
        ]
        
        function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
        keyword_pattern = r"\b(?:" + "|".join([f"\\b{func}\\b" for func in functions_as_keywords]) + r")\b"
        
    except ImportError as e:
        logger.error(f"Failed to import converter utilities: {str(e)}")
        raise
    
    for query_hash, query_text in batch_dict.items():
        query_start_time = time.time()
        processed_count += 1
        
        try:
            # Process using the EXACT same logic as the statistics endpoint
            query_text = normalize_unicode_spaces(query_text)
            item = "condenast"
            query_text, comment = strip_comment(query_text, item)
            
            # Extract functions from the query
            all_functions = extract_functions_from_query(
                query_text, function_pattern, keyword_pattern, exclusion_list
            )
            supported, unsupported = categorize_functions(
                all_functions, supported_functions_in_target, functions_as_keywords
            )

            udf_list, unsupported = extract_udfs(unsupported, from_dialect_functions)

            # Parse and analyze original query
            original_ast = parse_one(query_text, read=from_dialect)
            tables_list = extract_db_and_Table_names(original_ast)
            supported, unsupported = unsupported_functionality_identifiers(
                original_ast, unsupported, supported
            )
            values_ensured_ast = ensure_select_from_values(original_ast)
            cte_names_equivalence_ast = set_cte_names_case_sensitively(values_ensured_ast)
            normalized_query = cte_names_equivalence_ast.sql(from_dialect)
            
            # Transpile query
            tree = parse_one(normalized_query, read=from_dialect, error_level=None)
            tree2 = quote_identifiers(tree, dialect=to_dialect)
            values_ensured = ensure_select_from_values(tree2)
            cte_checked = set_cte_names_case_sensitively(values_ensured)
            converted_query = cte_checked.sql(
                dialect=to_dialect, from_dialect=from_dialect, pretty=True
            )
            converted_query = replace_struct_in_query(converted_query)
            converted_query = add_comment_to_query(converted_query, comment)
            
            # Analyze converted query
            all_functions_converted = extract_functions_from_query(
                converted_query, function_pattern, keyword_pattern, exclusion_list
            )
            supported_in_converted, unsupported_in_converted = categorize_functions(
                all_functions_converted, supported_functions_in_target, functions_as_keywords
            )
            
            converted_ast = parse_one(converted_query, read=to_dialect)
            supported_in_converted, unsupported_in_converted = unsupported_functionality_identifiers(
                converted_ast, unsupported_in_converted, supported_in_converted
            )
            
            joins_list = extract_joins_from_query(original_ast)
            cte_values_subquery_list = extract_cte_n_subquery_list(original_ast)
            
            executable = "NO" if unsupported_in_converted else "YES"
            
            processing_time_ms = int((time.time() - query_start_time) * 1000)
            
            result = {
                "query_hash": query_hash,
                "status": "success",
                "executable": executable,
                "from_dialect": from_dialect,
                "to_dialect": to_dialect,
                "original_query": query_text,
                "converted_query": converted_query,
                "supported_functions": list(supported),
                "unsupported_functions": list(unsupported),
                "udf_list": list(udf_list),
                "tables_list": list(tables_list),
                "joins_list": joins_list,
                "cte_values_subquery_list": cte_values_subquery_list,
                "unsupported_after_transpilation": list(unsupported_in_converted),
                "processing_time_ms": processing_time_ms,
                "error_message": ""
            }
            
            results.append(result)
            success_count += 1
            
        except Exception as e:
            error_msg = str(e)
            processing_time_ms = int((time.time() - query_start_time) * 1000)
            
            result = {
                "query_hash": query_hash,
                "status": "failed",
                "executable": "NO",
                "from_dialect": from_dialect,
                "to_dialect": to_dialect,
                "original_query": query_text,
                "converted_query": "",
                "supported_functions": [],
                "unsupported_functions": [],
                "udf_list": [],
                "tables_list": [],
                "joins_list": [],
                "cte_values_subquery_list": [],
                "unsupported_after_transpilation": [],
                "processing_time_ms": processing_time_ms,
                "error_message": error_msg
            }
            
            results.append(result)
            logger.debug(f"Query {query_hash} failed: {error_msg}")
        
        # Log progress every 1000 queries
        if processed_count % 1000 == 0:
            logger.info(f"Batch {batch_id}: {processed_count}/{len(batch_dict)} queries processed ({success_count} successful)")
    
    logger.info(f"Batch {batch_id} completed: {success_count}/{len(batch_dict)} queries successful")
    
    return results


def store_batch_results_to_iceberg(results: List[Dict[str, Any]], batch_id: str) -> bool:
    """
    Store batch results to Iceberg table
    
    Args:
        results: List of query processing results
        batch_id: Batch identifier
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Import the Iceberg storage function from converter_api
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
        
        from converter_api import store_query_in_iceberg
        from datetime import datetime
        
        stored_count = 0
        
        for i, result in enumerate(results):
            iceberg_data = {
                "query_id": i + 1,
                "batch_id": batch_id,
                "timestamp": datetime.now(),
                "status": result["status"],
                "executable": result["executable"],
                "from_dialect": result["from_dialect"],
                "to_dialect": result["to_dialect"],
                "original_query": result["original_query"],
                "converted_query": result["converted_query"],
                "supported_functions": set(result["supported_functions"]),
                "unsupported_functions": set(result["unsupported_functions"]),
                "udf_list": set(result["udf_list"]),
                "tables_list": set(result["tables_list"]),
                "processing_time_ms": result["processing_time_ms"],
                "error_message": result["error_message"]
            }
            
            if store_query_in_iceberg(iceberg_data):
                stored_count += 1
        
        logger.info(f"Stored {stored_count}/{len(results)} results to Iceberg for batch {batch_id}")
        return stored_count == len(results)
        
    except Exception as e:
        logger.error(f"Error storing batch {batch_id} to Iceberg: {str(e)}")
        return False


def process_modulo_batch_complete(
    file_path: str,
    query_column: str,
    remainder: int,
    total_batches: int,
    from_dialect: str,
    to_dialect: str,
    session_id: str,
    task_id: str
) -> Dict[str, Any]:
    """
    Complete modulo batch processing workflow:
    1. Create batch dictionary by filtering file
    2. Process all queries in the batch
    3. Store results to Iceberg
    
    Args:
        file_path: Path to parquet file
        query_column: Column containing queries
        remainder: Modulo remainder (0 to total_batches-1)
        total_batches: Total number of batches
        from_dialect: Source SQL dialect
        to_dialect: Target SQL dialect
        session_id: Session identifier
        task_id: Celery task identifier
        
    Returns:
        Processing results summary
    """
    batch_id = f"{session_id}_batch_{remainder}"
    start_time = time.time()
    
    logger.info(f"Starting modulo batch processing:")
    logger.info(f"  Batch ID: {batch_id}")
    logger.info(f"  File: {Path(file_path).name}")
    logger.info(f"  Modulo: {remainder} % {total_batches}")
    
    try:
        # Step 1: Create batch dictionary
        batch_dict = create_modulo_batch(file_path, query_column, remainder, total_batches)
        
        if not batch_dict:
            logger.warning(f"No queries found for batch {batch_id}")
            return {
                "batch_id": batch_id,
                "status": "completed",
                "unique_queries": 0,
                "successful_queries": 0,
                "failed_queries": 0,
                "processing_time_seconds": time.time() - start_time,
                "message": "No queries matched modulo condition"
            }
        
        # Step 2: Process all queries in the batch
        results = process_batch_queries(batch_dict, from_dialect, to_dialect, batch_id)
        
        # Step 3: Store results to Iceberg
        storage_success = store_batch_results_to_iceberg(results, batch_id)
        
        # Calculate statistics
        successful_queries = len([r for r in results if r["status"] == "success"])
        failed_queries = len([r for r in results if r["status"] == "failed"])
        executable_queries = len([r for r in results if r["executable"] == "YES"])
        
        processing_time = time.time() - start_time
        
        logger.info(f"Batch {batch_id} completed:")
        logger.info(f"  Unique queries: {len(batch_dict)}")
        logger.info(f"  Successful: {successful_queries}")
        logger.info(f"  Failed: {failed_queries}")
        logger.info(f"  Executable: {executable_queries}")
        logger.info(f"  Processing time: {processing_time:.2f}s")
        logger.info(f"  Stored to Iceberg: {storage_success}")
        
        return {
            "batch_id": batch_id,
            "status": "completed",
            "file_name": Path(file_path).name,
            "remainder": remainder,
            "total_batches": total_batches,
            "unique_queries": len(batch_dict),
            "successful_queries": successful_queries,
            "failed_queries": failed_queries,
            "executable_queries": executable_queries,
            "processing_time_seconds": processing_time,
            "iceberg_storage_success": storage_success,
            "queries_per_second": len(batch_dict) / processing_time if processing_time > 0 else 0
        }
        
    except Exception as e:
        error_msg = str(e)
        processing_time = time.time() - start_time
        
        logger.error(f"Batch {batch_id} failed after {processing_time:.2f}s: {error_msg}")
        
        return {
            "batch_id": batch_id,
            "status": "failed",
            "file_name": Path(file_path).name,
            "remainder": remainder,
            "total_batches": total_batches,
            "error_message": error_msg,
            "processing_time_seconds": processing_time
        }
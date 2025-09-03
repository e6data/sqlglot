"""
PyIceberg with AWS Glue Catalog - No Partitions
Simple implementation without partitioning to avoid SIGSEGV
"""

import logging
import os
import time
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.schema import Schema
from pyiceberg.types import *

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AWS Credentials - Hardcoded
os.environ["AWS_ACCESS_KEY_ID"] = "YOUR_ACCESS_KEY_ID"
os.environ["AWS_SECRET_ACCESS_KEY"] = "YOUR_SECRET_ACCESS_KEY"
os.environ["AWS_SESSION_TOKEN"] = "YOUR_SESSION_TOKEN"
os.environ["AWS_REGION"] = "us-east-1"

# Configuration
S3_BUCKET = "batch-transpiler"
S3_PREFIX = "testing-batch-processing"
GLUE_DATABASE = "batch_processing_db"
GLUE_TABLE = "batch_statistics"

# Global variables
iceberg_catalog = None
batch_statistics_table = None


def initialize_iceberg_catalog():
    """Initialize PyIceberg with Glue Catalog"""
    global iceberg_catalog, batch_statistics_table
    
    # Check if already initialized
    if iceberg_catalog is not None and batch_statistics_table is not None:
        logger.debug("PyIceberg already initialized, skipping")
        return True
    
    try:
        # Load Glue catalog only if not already loaded
        if iceberg_catalog is None:
            iceberg_catalog = load_catalog(
                "glue",
                **{
                    "type": "glue",
                    "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
                    "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
                    "aws_session_token": os.environ["AWS_SESSION_TOKEN"],
                    "region_name": os.environ["AWS_REGION"]
                }
            )
            logger.info("✅ PyIceberg Glue catalog loaded")
        
        # Try to load table only if not already loaded
        if batch_statistics_table is None:
            table_identifier = f"{GLUE_DATABASE}.{GLUE_TABLE}"
            try:
                batch_statistics_table = iceberg_catalog.load_table(table_identifier)
                logger.info(f"✅ Loaded existing table: {table_identifier}")
            except:
                # Create new table without partitions
                schema = Schema(
                    NestedField(1, "query_id", LongType(), required=True),
                    NestedField(2, "batch_id", StringType(), required=True),
                    NestedField(3, "company_name", StringType(), required=True),
                    NestedField(4, "event_date", StringType(), required=True),
                    NestedField(5, "batch_number", IntegerType(), required=True),
                    NestedField(6, "timestamp", TimestampType(), required=True),
                    NestedField(7, "status", StringType(), required=True),
                    NestedField(8, "executable", StringType(), required=True),
                    NestedField(9, "from_dialect", StringType(), required=True),
                    NestedField(10, "to_dialect", StringType(), required=True),
                    NestedField(11, "original_query", StringType(), required=True),
                    NestedField(12, "converted_query", StringType(), required=True),
                    NestedField(13, "supported_functions", ListType(element_id=101, element_type=StringType(), element_required=False), required=True),
                    NestedField(14, "unsupported_functions", ListType(element_id=102, element_type=StringType(), element_required=False), required=True),
                    NestedField(15, "udf_list", ListType(element_id=103, element_type=StringType(), element_required=False), required=True),
                    NestedField(16, "tables_list", ListType(element_id=104, element_type=StringType(), element_required=False), required=True),
                    NestedField(17, "processing_time_ms", LongType(), required=True),
                    NestedField(18, "error_message", StringType(), required=True),
                    NestedField(19, "unsupported_functions_after_transpilation", ListType(element_id=105, element_type=StringType(), element_required=False), required=True),
                    NestedField(20, "joins_list", ListType(element_id=106, element_type=StringType(), element_required=False), required=True)
                )
                
                # Create namespace if needed
                try:
                    iceberg_catalog.create_namespace(GLUE_DATABASE)
                except:
                    pass
                
                # Create table WITHOUT partitions
                location = f"s3://{S3_BUCKET}/{S3_PREFIX}/{GLUE_TABLE}/"
                batch_statistics_table = iceberg_catalog.create_table(
                    identifier=table_identifier,
                    schema=schema,
                    location=location
                    # NO partition_spec parameter
                )
                logger.info(f"✅ Created new table without partitions: {table_identifier}")
        
        return True
    
    except Exception as e:
        logger.error(f"❌ Failed to initialize PyIceberg: {e}")
        return False


def write_to_iceberg_with_retry(data, max_retries=3):
    """Write to PyIceberg Glue table with retry logic"""
    global batch_statistics_table
    
    if batch_statistics_table is None:
        if not initialize_iceberg_catalog():
            raise Exception("Failed to initialize Iceberg catalog")
    
    for attempt in range(max_retries):
        try:
            with batch_statistics_table.append() as write:
                write.write_dataframe(data)
            logger.info(f"✅ Successfully wrote data to Iceberg table")
            return  # Success
        except CommitFailedException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Commit failed, retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            logger.error(f"❌ Failed to write to Iceberg after {max_retries} attempts: {e}")
            raise


def write_to_iceberg(results_table, batch_data, max_retries=3):
    """Write to PyIceberg Glue table"""
    global batch_statistics_table
    
    if len(results_table) == 0:
        return True
    
    if batch_statistics_table is None:
        if not initialize_iceberg_catalog():
            return False
    
    try:
        # Add metadata columns
        enriched_table = _add_metadata_columns(results_table, batch_data)
        
        # Write to Iceberg table with retry logic
        for attempt in range(max_retries):
            try:
                batch_statistics_table.append(enriched_table)
                logger.info(f"✅ Wrote {len(results_table)} rows to PyIceberg table")
                return True
            except CommitFailedException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Commit failed, retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                logger.error(f"❌ PyIceberg write failed after {max_retries} attempts: {e}")
                return False
    
    except Exception as e:
        logger.error(f"❌ PyIceberg write failed: {e}")
        return False


def _add_metadata_columns(results_table, batch_data):
    """Add metadata columns to results table"""
    from datetime import datetime
    
    current_time = datetime.now()
    event_date = current_time.strftime("%Y-%m-%d")
    num_rows = len(results_table)
    
    # Handle missing columns
    empty_list = pa.array([[] for _ in range(num_rows)], type=pa.list_(pa.string()))
    
    # Create arrays and define schema with non-nullable fields
    arrays = [
        pa.array(list(range(1, num_rows + 1)), type=pa.int64()),
        pa.array([f"{batch_data['session_id']}_{batch_data['batch_id']}"] * num_rows),
        pa.array([batch_data.get('company_name', 'unknown')] * num_rows),
        pa.array([event_date] * num_rows),
        pa.array([batch_data.get('batch_idx', 0)] * num_rows, type=pa.int32()),
        pa.array([current_time] * num_rows, type=pa.timestamp('us')),
        results_table['status'],
        results_table['executable'],
        pa.array([batch_data.get('from_dialect', '')] * num_rows),
        pa.array([batch_data.get('to_dialect', '')] * num_rows),
        results_table['original_query'],
        results_table['converted_query'],
        results_table['supported_functions'],
        results_table['unsupported_functions'],
        results_table['udf_list'] if 'udf_list' in results_table.column_names else empty_list,
        results_table['tables_list'] if 'tables_list' in results_table.column_names else empty_list,
        results_table['processing_time_ms'],
        results_table['error_message'],
        results_table['unsupported_functions_after_transpilation'] if 'unsupported_functions_after_transpilation' in results_table.column_names else empty_list,
        results_table['joins_list'] if 'joins_list' in results_table.column_names else empty_list
    ]
    
    # Define schema with non-nullable fields
    schema = pa.schema([
        pa.field("query_id", pa.int64(), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("company_name", pa.string(), nullable=False),
        pa.field("event_date", pa.string(), nullable=False),
        pa.field("batch_number", pa.int32(), nullable=False),
        pa.field("timestamp", pa.timestamp('us'), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("executable", pa.string(), nullable=False),
        pa.field("from_dialect", pa.string(), nullable=False),
        pa.field("to_dialect", pa.string(), nullable=False),
        pa.field("original_query", pa.string(), nullable=False),
        pa.field("converted_query", pa.string(), nullable=False),
        pa.field("supported_functions", pa.list_(pa.string()), nullable=False),
        pa.field("unsupported_functions", pa.list_(pa.string()), nullable=False),
        pa.field("udf_list", pa.list_(pa.string()), nullable=False),
        pa.field("tables_list", pa.list_(pa.string()), nullable=False),
        pa.field("processing_time_ms", pa.int64(), nullable=False),
        pa.field("error_message", pa.string(), nullable=False),
        pa.field("unsupported_functions_after_transpilation", pa.list_(pa.string()), nullable=False),
        pa.field("joins_list", pa.list_(pa.string()), nullable=False)
    ])
    
    return pa.table(arrays, schema=schema)


# Initialize on import
initialize_iceberg_catalog()
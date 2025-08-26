"""
Iceberg Handler Module
Manages Iceberg table initialization and statistics
"""

import logging
import os
from typing import Dict, Any
from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, IntegerType, TimestampType, ListType, LongType
from pyiceberg.partitioning import PartitionSpec, PartitionField

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH", "/Users/niranjgaurav/PycharmProjects/sqlglot/automated_processing/iceberg_warehouse")
ICEBERG_CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "local_catalog")

# Global variables
iceberg_catalog = None
batch_statistics_table = None


def initialize_iceberg_catalog():
    """Initialize Iceberg catalog and create tables if they don't exist"""
    global iceberg_catalog, batch_statistics_table

    try:
        # Create warehouse directory if it doesn't exist
        warehouse_path = Path(ICEBERG_WAREHOUSE_PATH).absolute()
        warehouse_path.mkdir(parents=True, exist_ok=True)

        # SQLite database for catalog metadata
        catalog_db = warehouse_path / "catalog.db"

        iceberg_catalog = SqlCatalog(
            name=ICEBERG_CATALOG_NAME,
            uri=f"sqlite:///{catalog_db}",
            warehouse=f"file://{warehouse_path}"
        )

        # Define batch statistics table schema with partitioning fields
        batch_stats_schema = Schema(
            NestedField(1, "query_id", LongType(), required=False),
            NestedField(2, "batch_id", StringType(), required=False),
            NestedField(3, "company_name", StringType(), required=False),  # Partition field
            NestedField(4, "event_date", StringType(), required=False),  # Partition field (format: YYYY-MM-DD)
            NestedField(5, "batch_number", IntegerType(), required=False),  # Batch number for file naming
            NestedField(6, "timestamp", TimestampType(), required=False),
            NestedField(7, "status", StringType(), required=False),
            NestedField(8, "executable", StringType(), required=False),
            NestedField(9, "from_dialect", StringType(), required=False),
            NestedField(10, "to_dialect", StringType(), required=False),
            NestedField(11, "original_query", StringType(), required=False),
            NestedField(12, "converted_query", StringType(), required=False),
            NestedField(13, "supported_functions", ListType(element_id=20, element_type=StringType(), element_required=False), required=False),
            NestedField(14, "unsupported_functions", ListType(element_id=21, element_type=StringType(), element_required=False), required=False),
            NestedField(15, "udf_list", ListType(element_id=22, element_type=StringType(), element_required=False), required=False),
            NestedField(16, "tables_list", ListType(element_id=23, element_type=StringType(), element_required=False), required=False),
            NestedField(17, "processing_time_ms", LongType(), required=False),
            NestedField(18, "error_message", StringType(), required=False)
        )

        # Define partition specification for company_name and event_date
        partition_spec = PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform="identity", name="company_name"),
            PartitionField(source_id=4, field_id=1001, transform="identity", name="event_date")
        )

        # Create namespace if it doesn't exist
        namespace = "default"
        try:
            iceberg_catalog.create_namespace(namespace)
        except:
            pass  # Namespace might already exist

        # Create batch statistics table - try to load existing first
        table_identifier = f"{namespace}.batch_statistics"
        try:
            # First try to load existing table
            batch_statistics_table = iceberg_catalog.load_table(table_identifier)
            logger.info(f"Loaded existing Iceberg table: {table_identifier}")
            
            # Check if table has the new schema with company_name and event_date
            existing_columns = [field.name for field in batch_statistics_table.schema().fields]
            required_columns = ["company_name", "event_date", "batch_number"]
            missing_columns = [col for col in required_columns if col not in existing_columns]
            
            if missing_columns:
                logger.info(f"Table missing required columns: {missing_columns}. Dropping and recreating table...")
                try:
                    # Drop the existing table
                    iceberg_catalog.drop_table(table_identifier)
                    logger.info(f"Dropped existing table: {table_identifier}")
                    
                    # Create new table with updated schema and partitioning
                    batch_statistics_table = iceberg_catalog.create_table(
                        identifier=table_identifier,
                        schema=batch_stats_schema,
                        partition_spec=partition_spec
                    )
                    logger.info(f"Created new Iceberg table with updated schema: {table_identifier}")
                except Exception as recreate_error:
                    logger.error(f"Failed to recreate table: {str(recreate_error)}")

        except Exception as e:
            # Table doesn't exist, create a new one
            try:
                batch_statistics_table = iceberg_catalog.create_table(
                    identifier=table_identifier,
                    schema=batch_stats_schema,
                    partition_spec=partition_spec
                )
                logger.info(f"Created new Iceberg table: {table_identifier}")
            except Exception as create_error:
                logger.error(f"Failed to create Iceberg table: {str(create_error)}")
                batch_statistics_table = None

        logger.info("Iceberg catalog and tables initialized successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to initialize Iceberg catalog: {str(e)}")
        return False


# Initialize Iceberg catalog on module import
initialize_iceberg_catalog()
"""
Iceberg Handler Module
Manages all Iceberg table operations with concurrency control and retry logic
"""

import logging
import time
import threading
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, IntegerType, TimestampType, ListType, LongType, BooleanType
from contextlib import contextmanager
import redis
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global lock for Iceberg writes (process-level)
iceberg_write_lock = threading.Lock()

# Configuration
ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH", "/Users/niranjgaurav/PycharmProjects/sqlglot/distributed_processing/iceberg_warehouse")
ICEBERG_CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "local_catalog")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Global variables
iceberg_catalog = None
batch_statistics_table = None
query_counter = 0


def initialize_iceberg_catalog():
    """Initialize Iceberg catalog and create tables if they don't exist"""
    global iceberg_catalog, batch_statistics_table

    try:
        # Create warehouse directory if it doesn't exist
        warehouse_path = Path(ICEBERG_WAREHOUSE_PATH).absolute()
        warehouse_path.mkdir(parents=True, exist_ok=True)

        # Use SQL catalog for local storage
        from pyiceberg.catalog.sql import SqlCatalog

        # SQLite database for catalog metadata
        catalog_db = warehouse_path / "catalog.db"

        iceberg_catalog = SqlCatalog(
            name=ICEBERG_CATALOG_NAME,
            uri=f"sqlite:///{catalog_db}",
            warehouse=f"file://{warehouse_path}"
        )

        # Define batch statistics table schema using NestedField
        from pyiceberg.schema import NestedField

        batch_stats_schema = Schema(
            NestedField(1, "query_id", LongType(), required=False),
            NestedField(2, "batch_id", StringType(), required=False),
            NestedField(3, "timestamp", TimestampType(), required=False),
            NestedField(4, "status", StringType(), required=False),
            NestedField(5, "executable", StringType(), required=False),
            NestedField(6, "from_dialect", StringType(), required=False),
            NestedField(7, "to_dialect", StringType(), required=False),
            NestedField(8, "original_query", StringType(), required=False),
            NestedField(9, "converted_query", StringType(), required=False),
            NestedField(10, "supported_functions", ListType(element_id=16, element_type=StringType(), element_required=False), required=False),
            NestedField(11, "unsupported_functions", ListType(element_id=17, element_type=StringType(), element_required=False), required=False),
            NestedField(12, "udf_list", ListType(element_id=18, element_type=StringType(), element_required=False), required=False),
            NestedField(13, "tables_list", ListType(element_id=19, element_type=StringType(), element_required=False), required=False),
            NestedField(14, "processing_time_ms", LongType(), required=False),
            NestedField(15, "error_message", StringType(), required=False)
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

        except Exception as e:
            # Table doesn't exist, create a new one
            try:
                batch_statistics_table = iceberg_catalog.create_table(
                    identifier=table_identifier,
                    schema=batch_stats_schema
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


class IcebergWriter:
    """Thread-safe Iceberg writer with retry logic"""
    
    def __init__(self):
        self.redis_client = self._initialize_redis()
        self.max_retries = 5
        self.retry_delay = 10  # seconds
        
    def _initialize_redis(self):
        """Initialize Redis connection for distributed locking"""
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True
            )
            redis_client.ping()
            logger.info("Redis connection established for distributed locking")
            return redis_client
        except Exception as e:
            logger.warning(f"Redis not available, using local locking only: {e}")
            return None
    
    @contextmanager
    def distributed_lock(self, lock_name: str, timeout: int = 30):
        """
        Distributed lock using Redis with automatic release
        Falls back to local lock if Redis is not available
        """
        if self.redis_client:
            # Use Redis for distributed locking
            lock_key = f"iceberg_lock:{lock_name}"
            lock_value = f"{os.getpid()}_{threading.current_thread().ident}_{time.time()}"
            
            # Try to acquire lock with timeout
            acquired = False
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                if self.redis_client.set(lock_key, lock_value, nx=True, ex=timeout):
                    acquired = True
                    break
                time.sleep(0.1)  # Wait 100ms before retry
            
            if not acquired:
                raise TimeoutError(f"Could not acquire distributed lock {lock_name} within {timeout} seconds")
            
            try:
                yield
            finally:
                # Release lock only if we own it
                stored_value = self.redis_client.get(lock_key)
                if stored_value == lock_value:
                    self.redis_client.delete(lock_key)
        else:
            # Fall back to local lock
            with iceberg_write_lock:
                yield
    
    def store_query_with_retry(self, query_data: Dict[str, Any]) -> bool:
        """
        Store a single query result in the Iceberg table with retry logic
        """
        global batch_statistics_table, query_counter
        
        if batch_statistics_table is None:
            logger.warning("Iceberg table not initialized, skipping storage")
            return False
        
        for attempt in range(self.max_retries):
            try:
                # Use distributed lock for writing
                with self.distributed_lock(f"query_write_{query_data.get('batch_id', 'unknown')}", timeout=30):
                    
                    # Convert data to Iceberg format
                    records = [self._prepare_record(query_data)]
                    
                    # Create PyArrow table
                    arrow_table = self._create_arrow_table(records)
                    
                    # Append to Iceberg table
                    batch_statistics_table.append(arrow_table)
                    
                    query_counter += 1
                    
                    # Log progress
                    if query_counter % 100 == 0:
                        logger.info(f"Stored {query_counter} queries in Iceberg table")
                    
                    return True
                    
            except TimeoutError as e:
                logger.warning(f"Timeout acquiring lock (attempt {attempt + 1}/{self.max_retries}): {e}")
                time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                
            except Exception as e:
                if "concurrent" in str(e).lower() or "branch main" in str(e).lower():
                    # Concurrent write error - retry with backoff
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Concurrent write conflict (attempt {attempt + 1}/{self.max_retries}). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    
                    # Refresh table reference
                    try:
                        batch_statistics_table = iceberg_catalog.load_table("default.batch_statistics")
                    except:
                        pass
                else:
                    logger.error(f"Unexpected error storing query: {e}")
                    break
        
        logger.error(f"Failed to store query after {self.max_retries} attempts")
        return False
    
    def store_batch_results(self, batch_results: List[Dict[str, Any]], batch_id: str) -> int:
        """
        Store multiple query results in a single batch write
        Returns number of successfully stored results
        """
        global batch_statistics_table
        
        if not batch_results:
            return 0
        
        if batch_statistics_table is None:
            logger.warning("Iceberg table not initialized, skipping storage")
            return 0
        
        for attempt in range(self.max_retries):
            try:
                # Use distributed lock for batch writing
                with self.distributed_lock(f"batch_write_{batch_id}", timeout=60):
                    
                    # Prepare all records
                    records = [self._prepare_record(query_data) for query_data in batch_results]
                    
                    # Create PyArrow table
                    arrow_table = self._create_arrow_table(records)
                    
                    # Append to Iceberg table
                    batch_statistics_table.append(arrow_table)
                    
                    logger.info(f"Successfully stored {len(batch_results)} results for batch {batch_id}")
                    return len(batch_results)
                    
            except TimeoutError as e:
                logger.warning(f"Timeout acquiring lock for batch {batch_id} (attempt {attempt + 1}/{self.max_retries}): {e}")
                time.sleep(self.retry_delay * (2 ** attempt))
                
            except Exception as e:
                if "concurrent" in str(e).lower() or "branch main" in str(e).lower():
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Concurrent write conflict for batch {batch_id} (attempt {attempt + 1}/{self.max_retries}). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    
                    # Refresh table reference
                    try:
                        batch_statistics_table = iceberg_catalog.load_table("default.batch_statistics")
                    except:
                        pass
                else:
                    logger.error(f"Unexpected error storing batch {batch_id}: {e}")
                    break
        
        logger.error(f"Failed to store batch {batch_id} after {self.max_retries} attempts")
        return 0
    
    def _prepare_record(self, query_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a single record for Iceberg storage"""
        a = []
        # Keep lists as-is, don't add empty strings to avoid false positives
        supported_funcs = list(query_data.get("supported_functions", []))
        unsupported_funcs = list(query_data.get("unsupported_functions", []))
        udf_funcs = list(query_data.get("udf_list", []))
        table_names = list(query_data.get("tables_list", []))
        
        return {
            "query_id": query_data.get("query_id", 0),
            "batch_id": query_data.get("batch_id", "unknown"),
            "timestamp": query_data.get("timestamp", datetime.now()),
            "status": query_data.get("status", "unknown"),
            "executable": query_data.get("executable", "NO"),
            "from_dialect": query_data.get("from_dialect", "unknown"),
            "to_dialect": query_data.get("to_dialect", "e6"),
            "original_query": query_data.get("original_query", ""),
            "converted_query": query_data.get("converted_query", ""),
            "supported_functions": supported_funcs,
            "unsupported_functions": unsupported_funcs,
            "udf_list": udf_funcs,
            "tables_list": table_names,
            "processing_time_ms": query_data.get("processing_time_ms", 0),
            "error_message": query_data.get("error_message", "")
        }
    
    def _create_arrow_table(self, records: List[Dict[str, Any]]) -> pa.Table:
        """Create PyArrow table from records"""
        # Create PyArrow arrays with explicit types
        arrow_data = {
            "query_id": pa.array([r["query_id"] for r in records], type=pa.int64()),
            "batch_id": pa.array([r["batch_id"] for r in records], type=pa.string()),
            "timestamp": pa.array([r["timestamp"] for r in records], type=pa.timestamp('us')),
            "status": pa.array([r["status"] for r in records], type=pa.string()),
            "executable": pa.array([r["executable"] for r in records], type=pa.string()),
            "from_dialect": pa.array([r["from_dialect"] for r in records], type=pa.string()),
            "to_dialect": pa.array([r["to_dialect"] for r in records], type=pa.string()),
            "original_query": pa.array([r["original_query"] for r in records], type=pa.string()),
            "converted_query": pa.array([r["converted_query"] for r in records], type=pa.string()),
            "supported_functions": pa.array([r["supported_functions"] for r in records], type=pa.list_(pa.string())),
            "unsupported_functions": pa.array([r["unsupported_functions"] for r in records], type=pa.list_(pa.string())),
            "udf_list": pa.array([r["udf_list"] for r in records], type=pa.list_(pa.string())),
            "tables_list": pa.array([r["tables_list"] for r in records], type=pa.list_(pa.string())),
            "processing_time_ms": pa.array([r["processing_time_ms"] for r in records], type=pa.int64()),
            "error_message": pa.array([r["error_message"] for r in records], type=pa.string())
        }
        
        return pa.Table.from_arrays(list(arrow_data.values()), names=list(arrow_data.keys()))


def store_query_in_iceberg(query_data: Dict[str, Any]) -> bool:
    """
    Public function to store a single query result
    Uses IcebergWriter with retry logic
    """
    writer = IcebergWriter()
    return writer.store_query_with_retry(query_data)


def store_batch_in_iceberg(batch_results: List[Dict[str, Any]], batch_id: str) -> int:
    """
    Public function to store batch results
    Uses IcebergWriter with retry logic
    Returns number of successfully stored results
    """
    writer = IcebergWriter()
    return writer.store_batch_results(batch_results, batch_id)


def get_iceberg_table_stats() -> Dict[str, Any]:
    """Get statistics about the Iceberg table"""
    global batch_statistics_table

    try:
        if batch_statistics_table is None:
            return {"error": "Table not initialized"}

        # Get table metadata
        table_scan = batch_statistics_table.scan()
        df = table_scan.to_pandas()

        return {
            "total_rows": len(df),
            "total_successful": len(df[df['status'] == 'success']),
            "total_failed": len(df[df['status'] == 'failed']),
            "executable_queries": len(df[df['executable'] == 'YES']),
            "unique_dialects": df['from_dialect'].nunique(),
            "latest_timestamp": df['timestamp'].max() if not df.empty else None
        }

    except Exception as e:
        logger.error(f"Failed to get Iceberg table stats: {str(e)}")
        return {"error": str(e)}


# Initialize Iceberg catalog on module import
initialize_iceberg_catalog()
import e6_metadata_common.ttypes as ttypes
import e6_storage_service.StorageService as StorageService
import e6_storage_service.ttypes as ttypes
from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from thrift.transport.TTransport import TTransportException
from thrift.transport import TSocket
import sqlglot
from .rules_validator import validate_queries_dynamic 
from .extract import extract_sql_components_per_table_with_alias
from .rules_validator import validate_queries


class StorageServiceClient:
    def __init__(self, host='localhost', port=9006, timeout=1000):
        
        """
        Initialize the StorageService client with connection parameters.
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None
        self.storage = None
        self.connect()
    
    def connect(self):
        """Establish connection to the storage service"""
        try:
            self.socket = TSocket.TSocket(self.host, self.port)
            self.socket.setTimeout(self.timeout)
            self.socket.open()
            
            protocol = TBinaryProtocol.TBinaryProtocol(self.socket)
            storage_service = TMultiplexedProtocol.TMultiplexedProtocol(protocol, "StorageService")
            self.storage = StorageService.Client(storage_service)
            
        except TTransportException as e:
            raise ConnectionError(f"Failed to connect to storage service: {str(e)}")
    
    def reconnect(self):
        """Reconnect to the storage service if connection is lost"""
        self.close()
        self.connect()
    
    def close(self):
        """Close the connection to the storage service"""
        if self.socket:
            self.socket.close()
            self.socket = None
            self.storage = None
    
    def ensure_connection(self):
        """Ensure that the connection is active"""
        if not self.socket or not self.socket.isOpen():
            self.connect()
    
    def get_catalog_names(self):
        """Get list of catalog names"""
        self.ensure_connection()
        try:
            return self.storage.getCatalogNames()
        except Exception as e:
            self.reconnect()
            return self.storage.getCatalogNames()
    
    def get_db_names(self, catalog_name):
        """
        Get list of database names for a catalog        
        """
        self.ensure_connection()
        try:
            return self.storage.getDBNames(catalog_name)
        except Exception as e:
            self.reconnect()
            return self.storage.getDBNames(catalog_name)
    
    def get_table_names(self, catalog_name, db_name):
        """
        Get list of table names for a database
        
        Args:
            catalog_name (str): Name of the catalog
            db_name (str): Name of the database
        """
        self.ensure_connection()
        try:
            return self.storage.getTableNames(catalog_name, db_name)
        except Exception as e:
            self.reconnect()
            return self.storage.getTableNames(catalog_name, db_name)
    
    def get_columns(self, catalog_name, db_name, table_name):
        """
        Get list of columns for a table
        
        """
        self.ensure_connection()
        try:
            return self.storage.getColumns(catalog_name, db_name, table_name)
        except Exception as e:
            self.reconnect()
            return self.storage.getColumns(catalog_name, db_name, table_name)
    
    def get_table_info(self, catalog_name="hive", db_name="db", table_name="db"):
        """
        Get complete information about a table including catalogs, databases, tables and columns
        """
        return {
            'catalog_names': self.get_catalog_names(),
            'databases': self.get_db_names(catalog_name),
            'tables': self.get_table_names(catalog_name, db_name),
            'columns': self.get_columns(catalog_name, db_name, table_name)
        }

    def get_partition_info(self, catalog_name, db_name, table_name):
        """
        Get partition information for a table
        
        """
        self.ensure_connection()
        try:
            return self.storage.getTablePartitions(catalogName=catalog_name, dbName=db_name, tableName=table_name,requestId="1234",forceRefresh=False,lastUpdateTimeFromCache=0)
        except Exception as e:
            self.reconnect()
            return self.storage.getTablePartitions(catalogName=catalog_name, dbName=db_name, tableName=table_name,requestId="1234",forceRefresh=False,lastUpdateTimeFromCache=0)
            # return self.storage.getTablePartitions(catalog_name, db_name, table_name,"",False,0)

    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def Get_table_info(sql_query,catalog,db):
    client = StorageServiceClient()
    thing = sqlglot.parse(sql=sql_query, error_level=None)
    
    # do stuff get a list of columns

    # get table and partition info
    
    # get table info

    try:
        return client.get_table_info()

    finally:
        client.close()

def get_table_infos(tables,client,catalog="hive",schema="tpcds_1000"):
    """
    Get detailed table information for a list of table names
    
    Args:
        tables (List[str]): List of table names
        
    Returns:
        Dict[str, Dict]: Dictionary containing table information with the following structure:
            {
                "table_name": {
                    "column_count": int,
                    "columns": [
                        {
                            "name": str,
                            "type": str
                        },
                        ...
                    ],
                    "partition_values": List[str]
                },
                ...
            }
    """
    # client = StorageServiceClient()
    table_infos = {}
    
    try:
        for table_name in tables:
            table_info = {
                "column_count": 0,
                "columns": [],
                "partition_values": []
            }
            
            # Get column information
            columns = client.get_columns(catalog,schema,table_name)
            if columns:
                column_list = []
                for col in columns:
                    column_list.append({
                        "name": col.fieldName,
                        "type": col.fieldType
                    })
                table_info["columns"] = column_list
                table_info["column_count"] = len(column_list)
            
            # Get partition information
            partitions = client.get_partition_info("hive", "tpcds_1000", table_name)
            if partitions and partitions.partitions:
                # Assuming the first partition contains the field names
                # since they should be consistent across all partitions
                table_info["partition_values"] = partitions.partitions[0].fieldNames
            
            table_infos[table_name] = table_info            
    except Exception as e:
        print(f"Error processing table information: {str(e)}")
    finally:
        client.close()
        
    return table_infos

if __name__ == "__main__":
    client = StorageServiceClient()
    try:

        sql = """
            SELECT 
                *,
                d.d_year,
                d.d_month_seq,
                SUM(ws.ws_net_paid) as total_net_sales,
                COUNT(DISTINCT ws.ws_order_number) as number_of_orders,
                AVG(ws.ws_net_profit) as avg_profit_per_sale
            FROM 
                web_sales ws
                JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
            WHERE 
                d.d_year = 2023
                AND d.d_holiday = 'N'
                AND ws.ws_net_paid > 0
                AND ws.ws_quantity > 0
            GROUP BY 
                d.d_year,
                d.d_month_seq
            HAVING 
                SUM(ws.ws_net_paid) > 1000
            ORDER BY 
                d.d_month_seq;        
                """
        
        print("SQL is",sql,"\n")
        parsed = sqlglot.parse(sql, error_level=None)
        # print("\nParsed is\n",parsed)
        queries , tables = extract_sql_components_per_table_with_alias(parsed) 
        print("\Extracted info from query is \n",queries)
        # tables = client.get_table_names(catalog_name="hive", db_name="tpcds_1000")
        table_map = [] #get_table_infos(tables)    
        # print("\nInfo is\n",info)
        print("\nGot info from Storage Service for tables -> ",tables,"\n")    
        violations_found = validate_queries(queries, table_map)
        
        if violations_found:
            print("Violations Detected:")
            for v in violations_found:
                print(f"Query {v['query_index']} on table '{v['table']}': {v['violation']}")
        else:
            print("No violations found.")

        # for name in db:
        #     col = client.get_columns("hive","tpcds_1000",name)
        #     for j in col:
        #         print("column-> ",j.fieldName," \n")

        #     print("\n\n\n")
        #     partitions = client.get_partition_info("hive","tpcds_1000",name)
        #     if partitions is not None:
        #         print("len is ",len(partitions.partitions))
        #         for p in partitions.partitions:
        #             field_names = p.fieldNames
        #             field_values = [v.value for v in p.fieldValues]
        #             for name, value in zip(field_names, field_values):
        #                 print(f"{name} = {value}")

        #             print(p)
        #             break
    finally:
        client.close()


# violations_found = validate_queries(queries, table_map)

# # Display violations
# if violations_found:
#     print("Violations Detected:")
#     for v in violations_found:
#         print(f"Query {v['query_index']} on table '{v['table']}': {v['violation']}")
# else:
#     print("No violations found.")

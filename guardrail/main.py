import e6_metadata_common.ttypes as ttypes
import e6_storage_service.StorageService as StorageService
import e6_storage_service.ttypes as ttypes
from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from thrift.transport.TTransport import TTransportException
from thrift.transport import TSocket
import sqlglot

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
    
    def get_table_info(self, catalog_name="hivecatalog", db_name="experiments_db_iceberg", table_name="ultron_array_table"):
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

if __name__ == "__main__":
    client = StorageServiceClient()
    try:
        col = client.get_columns("hive","tpcds_1000","item")
        for j in col:
            print("column-> ",j," \n")

        print("\n\n\n")
        partitions = client.get_partition_info("hive","tpcds_1000","item")
        print(partitions)
        
    finally:
        client.close()

# GetTablePartitionsResponse(partitions=[E6PartitionInfo(filePathName='s3a://tpcds-datagen/tpcds-1000/item', fieldNames=[], fieldValues=[], fieldTypes=[], containsRecordCount=False, virtualPartitionId=None, virtualPartitionVersionId=None)], tableSizeInBytes=None, lastUpdateTimeMillis=1735999136844)

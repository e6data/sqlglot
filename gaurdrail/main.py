import e6_metadata_common.ttypes as ttypes
import e6_storage_service.StorageService as StorageService
import e6_storage_service.ttypes as ttypes
from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from thrift.transport.TTransport import TTransportException
from thrift.transport import TSocket



class StorageServiceClient:
    def __init__(self, host='localhost', port=9005, timeout=1000):
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
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()



if __name__ == "__main__":
    client = StorageServiceClient()
    try:
        columns = client.get_db_names("hivecatalog")
        print(columns)
        # Do more operations...
    finally:
        client.close()
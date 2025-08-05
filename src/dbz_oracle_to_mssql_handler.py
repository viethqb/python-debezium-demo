import os
from pathlib import Path
import uuid
import json
import pyodbc

from typing import List
from pydbzengine import ChangeEvent, BasePythonChangeHandler
from pydbzengine import Properties, DebeziumJsonEngine

OFFSET_FILE = os.getenv("OFFSET_FILE", "/app/storage/offsets.dat")
HISTORY_FILE = os.getenv("HISTORY_FILE", "/app/storage/history.dat")
CLEAR_OFFSET_AND_HISTORY_FILE = os.getenv("CLEAR_OFFSET_AND_HISTORY_FILE", "False").lower() == "true"
print("CLEAR_OFFSET_AND_HISTORY_FILE: " + str(CLEAR_OFFSET_AND_HISTORY_FILE))

if CLEAR_OFFSET_AND_HISTORY_FILE:
    try:
        os.remove(OFFSET_FILE)
        print(f"Successfully removed offset file: {OFFSET_FILE}")
    except OSError as e:
        print(f"Error removing offset file {OFFSET_FILE}: {e}")
    
    try:
        os.remove(HISTORY_FILE)
        print(f"Successfully removed history file: {HISTORY_FILE}")
    except OSError as e:
        print(f"Error removing history file {HISTORY_FILE}: {e}")

ORACLE_HOST = os.getenv("ORACLE_HOST", "oracle")
ORACLE_PORT = os.getenv("ORACLE_PORT", 1521)
ORACLE_USER = os.getenv("ORACLE_USER", "c##dbzuser")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "dbz")
ORACLE_DBNAME = os.getenv("ORACLE_DBNAME", "ORCLCDB")
ORACLE_PDB_NAME = os.getenv("ORACLE_PDB_NAME", "ORCLPDB1")
ORACLE_TABLE_INCLUDE_LIST = os.getenv("ORACLE_TABLE_INCLUDE_LIST", "C##DBZUSER.CUSTOMERS")
MSSQL_HOST = os.getenv("MSSQL_HOST", "mssql")
MSSQL_PORT = os.getenv("MSSQL_PORT", 1433)
MSSQL_USER = os.getenv("MSSQL_USER", "SA")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "mssql123AA")
MSSQL_DB = os.getenv("MSSQL_DB", "raw_db")
MSSQL_INSERT_BATCH_SIZE = os.getenv("MSSQL_INSERT_BATCH_SIZE", 1000)



class RawChangeHandler(BasePythonChangeHandler):
    """
    A custom change event handler that stores raw Debezium events in PostgreSQL.
    The target table has columns: uuid, destination, key, value.
    """
    BATCH_SIZE = int(MSSQL_INSERT_BATCH_SIZE)

    def __init__(self):
        super().__init__()
        # Initialize MSSQL connection
        self.conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
            f"DATABASE={MSSQL_DB};"
            f"UID={MSSQL_USER};"
            f"PWD={MSSQL_PASSWORD};"
            "TrustServerCertificate=yes;"
        )
        
        try:
            self.mssql_conn = pyodbc.connect(self.conn_str)
            self.mssql_conn.autocommit = False  # Enable transactions
            self.mssql_cursor = self.mssql_conn.cursor()
            
            # Ensure the raw_events table exists
            self._create_raw_events_table()
        except pyodbc.Error as e:
            print(f"Error connecting to MSSQL: {str(e)}")
            raise
    
    def __del__(self):
        # Clean up MSSQL connection when the handler is destroyed
        if hasattr(self, 'mssql_cursor'):
            self.mssql_cursor.close()
        if hasattr(self, 'mssql_conn'):
            self.mssql_conn.close()
    
    def _create_raw_events_table(self):
        """Create the raw_events table if it doesn't exist"""
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'raw_events')
        BEGIN
            CREATE TABLE raw_events (
                uuid UNIQUEIDENTIFIER PRIMARY KEY,
                destination NVARCHAR(255),
                [key] NVARCHAR(MAX),
                [value] NVARCHAR(MAX),
                processed_at DATETIME DEFAULT GETDATE()
            )
        END
        """
        try:
            self.mssql_cursor.execute(create_table_query)
            self.mssql_conn.commit()
        except pyodbc.Error as e:
            print(f"Error creating table: {str(e)}")
            self.mssql_conn.rollback()
            raise

    def _insert_batch(self, batch_data):
        """Helper method to insert a batch of records"""
        if not batch_data:
            return
        
        # Using parameterized query with pyodbc's parameter style
        insert_query = """
        INSERT INTO raw_events (uuid, destination, [key], [value])
        VALUES (?, ?, ?, ?)
        """
        
        try:
            self.mssql_cursor.executemany(insert_query, batch_data)
            self.mssql_conn.commit()
            print(f"Successfully stored {len(batch_data)} records")
        except pyodbc.Error as e:
            print(f"Error processing batch: {str(e)}")
            self.mssql_conn.rollback()
            raise

    def handleJsonBatch(self, records: List[ChangeEvent]):
        """
        Handles a batch of Debezium change events by storing them raw in MSSQL.
        Process records in batches of BATCH_SIZE.
        """
        print(f"Processing {len(records)} records")
        
        try:
            batch_data = []
            for record in records:
                # Generate a unique UUID for each record
                record_uuid = str(uuid.uuid4())  # MSSQL expects string representation
                
                # Get the raw data from the ChangeEvent
                destination = record.destination()
                # key = json.dumps(record.key()) if record.key() else None
                # value = json.dumps(record.value()) if record.value() else None

                key = record.key() if record.key() else None
                value = record.value() if record.value() else None
                
                batch_data.append((
                    record_uuid,
                    destination,
                    key,
                    value
                ))
                
                # If batch size reached, insert
                if len(batch_data) >= self.BATCH_SIZE:
                    self._insert_batch(batch_data)
                    batch_data = []  # Reset batch
            
            # Insert remaining records
            if batch_data:
                self._insert_batch(batch_data)
            
        except Exception as e:
            print(f"Error processing records: {str(e)}")
            # Rollback on error
            if hasattr(self, 'mssql_conn'):
                self.mssql_conn.rollback()
            raise


if __name__ == '__main__':
    props = Properties()
    props.setProperty("name", "engine")
    props.setProperty("snapshot.mode", "schema_only")
    props.setProperty("topic.prefix", "dwh")
    props.setProperty("tombstones.on.delete", "false")
    props.setProperty("log.mining.strategy", "online_catalog")
    props.setProperty("database.connection.adapter", "logminer")
    # props.setProperty("log.mining.batch.size.min", "1")
    # props.setProperty("log.mining.batch.size.default", "500")
    # props.setProperty("log.mining.batch.size.max", "1000")
    props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector")
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    props.setProperty("offset.storage.file.filename", OFFSET_FILE)
    props.setProperty("offset.flush.interval.ms", "1000")
    props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
    props.setProperty("schema.history.internal.file.filename", HISTORY_FILE)
    props.setProperty("tasks.max", "1")
    props.setProperty("database.hostname", ORACLE_HOST)
    props.setProperty("database.port", ORACLE_PORT)
    props.setProperty("database.user", ORACLE_USER)
    props.setProperty("database.password", ORACLE_PASSWORD)
    props.setProperty("database.dbname", ORACLE_DBNAME)
    props.setProperty("database.pdb.name", ORACLE_PDB_NAME)
    props.setProperty("database.server.name", "server1")
    props.setProperty("table.include.list", ORACLE_TABLE_INCLUDE_LIST)
    props.setProperty("table.whitelist", ORACLE_TABLE_INCLUDE_LIST)
    props.setProperty("poll.interval.ms", "1000")
    # props.setProperty("schema.history.internal.skip.unparseable.ddl", "true")
    # props.setProperty("schema.history.history.skip.unparseable.ddl", "true")
    props.setProperty("decimal.handling.mode", "string")

    # props.setProperty("transforms", "unwrap")
    # props.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
    # props.setProperty("transforms.unwrap.add.fields", "op,table,source.ts_ms,sourcedb,ts_ms")
    # props.setProperty("transforms.unwrap.delete.handling.mode", "rewrite")

    # Create a DebeziumJsonEngine instance, passing the configuration properties and the custom change event handler.
    engine = DebeziumJsonEngine(properties=props, handler=RawChangeHandler())
    # Start the Debezium engine to begin consuming and processing change events.
    engine.run()

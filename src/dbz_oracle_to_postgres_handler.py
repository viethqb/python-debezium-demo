import os
from pathlib import Path
import psycopg2
from psycopg2 import sql
import uuid
import json

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
POSTGRESQL_HOST = os.getenv("POSTGRESQL_HOST", "postgres")
POSTGRESQL_PORT = os.getenv("POSTGRESQL_PORT", 5432)
POSTGRESQL_USER = os.getenv("POSTGRESQL_USER", "postgres")
POSTGRESQL_PASSWORD = os.getenv("POSTGRESQL_PASSWORD", "postgres123AA")
POSTGRESQL_DB = os.getenv("POSTGRESQL_DB", "postgres")
POSTGRESQL_INSERT_BATCH_SIZE = os.getenv("POSTGRESQL_INSERT_BATCH_SIZE", 1000)



class RawChangeHandler(BasePythonChangeHandler):
    """
    A custom change event handler that stores raw Debezium events in PostgreSQL.
    The target table has columns: uuid, destination, key, value.
    """
    BATCH_SIZE = int(POSTGRESQL_INSERT_BATCH_SIZE)

    def __init__(self):
        super().__init__()
        # Initialize PostgreSQL connection
        self.pg_conn = psycopg2.connect(
            host=POSTGRESQL_HOST,
            port=POSTGRESQL_PORT,
            database=POSTGRESQL_DB,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD
        )
        self.pg_cursor = self.pg_conn.cursor()
        
        # Ensure the raw_events table exists
        self._create_raw_events_table()
    
    def __del__(self):
        # Clean up PostgreSQL connection when the handler is destroyed
        if hasattr(self, 'pg_cursor'):
            self.pg_cursor.close()
        if hasattr(self, 'pg_conn'):
            self.pg_conn.close()
    
    def _create_raw_events_table(self):
        """Create the raw_events table if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_events (
            uuid UUID PRIMARY KEY,
            destination TEXT,
            key JSONB,
            value JSONB,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.pg_cursor.execute(create_table_query)
        self.pg_conn.commit()

    def _insert_batch(self, batch_data):
        """Helper method to insert a batch of records"""
        if not batch_data:
            return
        
        insert_query = """
        INSERT INTO raw_events (uuid, destination, key, value)
        VALUES (%s, %s, %s, %s)
        """
        
        try:
            self.pg_cursor.executemany(insert_query, batch_data)
            self.pg_conn.commit()
            print(f"Successfully stored {len(batch_data)} records")
        except Exception as e:
            print(f"Error processing batch: {str(e)}")
            self.pg_conn.rollback()
            raise

    def handleJsonBatch(self, records: List[ChangeEvent]):
        """
        Handles a batch of Debezium change events by storing them raw in PostgreSQL.
        """

        print(f"Processing {len(records)} records")
        
        try:
            batch_data = []
            for record in records:
                # Generate a unique UUID for each record
                record_uuid = uuid.uuid4()
                
                # Get the raw data from the ChangeEvent
                destination = record.destination()
                key = json.dumps(record.key()) if record.key() else None
                value = json.dumps(record.value()) if record.value() else None
                
                batch_data.append((
                    str(record_uuid),
                    destination,
                    key,
                    value
                ))
                
                # print(f"Storing record: {destination} | {key} | {value}")

                if len(batch_data) >= self.BATCH_SIZE:
                    self._insert_batch(batch_data)
                    batch_data = []  # Reset batch
            
            # Insert các record còn lại sau khi vòng lặp kết thúc
            if batch_data:
                self._insert_batch(batch_data)
                        
        except Exception as e:
            print(f"Error processing records: {str(e)}")
            self.pg_conn.rollback()
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

import os
from pathlib import Path

from typing import List
from pydbzengine import ChangeEvent, BasePythonChangeHandler
from pydbzengine import Properties, DebeziumJsonEngine

OFFSET_FILE = "/app/storage/offsets.dat"
HISTORY_FILE = "/app/storage/history.dat"
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



class PrintChangeHandler(BasePythonChangeHandler):
    """
    A custom change event handler class.

    This class processes batches of Debezium change events received from the engine.
    The `handleJsonBatch` method is where you implement your logic for consuming
    and processing these events.  Currently, it prints basic information about
    each event to the console.
    """

    def handleJsonBatch(self, records: List[ChangeEvent]):
        """
        Handles a batch of Debezium change events.

        This method is called by the Debezium engine with a list of ChangeEvent objects.
        Change this method to implement your desired processing logic.  For example,
        you might parse the event data, transform it, and load it into a database or
        other destination.

        Args:
            records: A list of ChangeEvent objects representing the changes captured by Debezium.
        """
        print("--------------------------------------")
        print(f"Received {len(records)} records")
        for record in records:
            print(f"destination: {record.destination()}")
            print(f"key: {record.key()}")
            print(f"value: {record.value()}")
        print("--------------------------------------")


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
    engine = DebeziumJsonEngine(properties=props, handler=PrintChangeHandler())

    # Start the Debezium engine to begin consuming and processing change events.
    engine.run()

import os
from pathlib import Path

from typing import List
from pydbzengine import Properties, DebeziumJsonEngine
from pydbzengine.handlers.dlt import DltChangeHandler
from pydbzengine.helper import Utils
import dlt

OFFSET_FILE = "/app/storage/offsets.dat"
HISTORY_FILE = "/app/storage/history.dat"
DUCKDB_FILE = "/app/storage/dbz_cdc_events_example.duckdb"
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
    # props.setProperty("max.batch.size", "5")
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

    dlt_pipeline = dlt.pipeline(
        pipeline_name="dbz_cdc_events_example",
        destination="duckdb",
        dataset_name="dbz_data"
    )

    handler = DltChangeHandler(dlt_pipeline=dlt_pipeline)
    # Create a DebeziumJsonEngine instance, passing the configuration properties and the custom change event handler.
    engine = DebeziumJsonEngine(properties=props, handler=handler)

    # Start the Debezium engine to begin consuming and processing change events.
    Utils.run_engine_async(engine=engine, timeout_sec=60)
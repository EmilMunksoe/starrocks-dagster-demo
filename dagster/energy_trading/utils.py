"""Shared utilities for StarRocks connections and queries"""

import pymysql
from contextlib import contextmanager
from typing import Optional, List, Tuple

from dagster import AssetExecutionContext

from .config import (
    STARROCKS_HOST,
    STARROCKS_PORT,
    STARROCKS_USER,
    STARROCKS_PASSWORD,
    HIVE_METASTORE_HOST,
    HIVE_METASTORE_PORT,
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_CONTAINER,
)


@contextmanager
def get_starrocks_connection(database: Optional[str] = None):
    """Context manager for StarRocks connections

    Args:
        database: Optional database to connect to

    Yields:
        tuple: (connection, cursor)
    """
    conn = pymysql.connect(
        host=STARROCKS_HOST,
        port=STARROCKS_PORT,
        user=STARROCKS_USER,
        password="" if STARROCKS_PASSWORD == "root" else STARROCKS_PASSWORD,
        database=database,
        charset="utf8mb4",
    )
    cursor = conn.cursor()

    try:
        yield conn, cursor
    finally:
        cursor.close()
        conn.close()


def execute_starrocks_query(query: str, database: Optional[str] = None) -> list:
    """Execute a StarRocks query and return results

    Args:
        query: SQL query to execute
        database: Optional database to connect to

    Returns:
        list: Query results as list of tuples
    """
    with get_starrocks_connection(database) as (conn, cursor):
        cursor.execute(query)
        return cursor.fetchall()


def execute_starrocks_ddl(query: str, database: Optional[str] = None) -> None:
    """Execute a StarRocks DDL statement (CREATE, ALTER, DROP, etc.)

    Args:
        query: DDL statement to execute
        database: Optional database to connect to
    """
    with get_starrocks_connection(database) as (conn, cursor):
        cursor.execute(query)
        conn.commit()


def register_delta_table_in_hive_metastore(
    context: AssetExecutionContext,
    namespace: str,
    table_name: str,
    storage_location: str,
    columns: List[Tuple[str, str, str]],
    drop_if_exists: bool = False,
) -> None:
    """Register a Delta Lake table in Hive Metastore via Thrift

    Args:
        context: Dagster execution context for logging
        namespace: Database/schema name in Hive
        table_name: Table name
        storage_location: Azure Storage location (abfss:// path)
        columns: List of (name, type, comment) tuples defining table schema
        drop_if_exists: Whether to drop existing table before creating
    """
    try:
        from hmsclient import HMSClient
        from hmsclient.genthrift.hive_metastore.ttypes import (
            Database,
            Table,
            StorageDescriptor,
            SerDeInfo,
            FieldSchema,
        )

        context.log.info(
            f"Connecting to Hive Metastore at {HIVE_METASTORE_HOST}:{HIVE_METASTORE_PORT}"
        )

        client = HMSClient(host=HIVE_METASTORE_HOST, port=HIVE_METASTORE_PORT)
        client.open()

        # Create database if not exists
        try:
            client.get_database(namespace)
            context.log.info(f"Database '{namespace}' already exists")
        except Exception:
            context.log.info(f"Creating database '{namespace}'")
            db = Database(
                name=namespace,
                description=f"Database for {namespace} tables",
                locationUri=f"abfss://{AZURE_STORAGE_CONTAINER}@{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{namespace}",
                parameters={},
            )
            client.create_database(db)

        # Handle existing table
        try:
            client.get_table(dbname=namespace, tbl_name=table_name)
            if drop_if_exists:
                context.log.info(f"Table {namespace}.{table_name} exists, dropping it")
                client.drop_table(dbname=namespace, tbl_name=table_name)
            else:
                context.log.info(f"Table {namespace}.{table_name} already registered")
                client.close()
                return
        except Exception:
            context.log.info(f"Table {namespace}.{table_name} does not exist yet")

        # Define table schema
        cols = [
            FieldSchema(name=name, type=col_type, comment=comment)
            for name, col_type, comment in columns
        ]

        # Define storage descriptor
        sd = StorageDescriptor(
            cols=cols,
            location=storage_location,
            inputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            outputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            compressed=False,
            numBuckets=-1,
            serdeInfo=SerDeInfo(
                name=table_name,
                serializationLib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                parameters={},
            ),
            bucketCols=[],
            sortCols=[],
            parameters={},
        )

        # Create table object
        table = Table(
            tableName=table_name,
            dbName=namespace,
            owner="dagster",
            createTime=0,
            lastAccessTime=0,
            retention=0,
            sd=sd,
            partitionKeys=[],
            parameters={"EXTERNAL": "TRUE"},
            tableType="EXTERNAL_TABLE",
        )

        # Register table
        context.log.info(
            f"Registering table '{namespace}.{table_name}' in Hive Metastore"
        )
        client.create_table(table)
        context.log.info(f"✅ Successfully registered table {namespace}.{table_name}")

        # Verify
        registered_table = client.get_table(dbname=namespace, tbl_name=table_name)
        context.log.info(f"Verified table location: {registered_table.sd.location}")

        client.close()

    except ImportError:
        context.log.warning(
            "hmsclient not installed, skipping Hive Metastore registration"
        )
        context.log.info(f"Delta Lake table written to: {storage_location}")
    except Exception as e:
        context.log.warning(f"Could not register table in Hive Metastore: {e}")
        context.log.info(f"Delta Lake table written to: {storage_location}")

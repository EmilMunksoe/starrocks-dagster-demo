"""Shared utilities for StarRocks connections and queries"""
import pymysql
from contextlib import contextmanager
from typing import Optional

from .config import (
    STARROCKS_HOST,
    STARROCKS_PORT,
    STARROCKS_USER,
    STARROCKS_PASSWORD
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
        password='' if STARROCKS_PASSWORD == 'root' else STARROCKS_PASSWORD,
        database=database,
        charset='utf8mb4'
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

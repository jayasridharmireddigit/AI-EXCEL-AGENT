from typing import Dict, List, Tuple, Optional, Any

import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table, select
from sqlalchemy.engine import Engine


def get_engine(conn_str: str) -> Engine:
    """
    Create a SQLAlchemy engine from a DB connection string.

    Examples of conn_str values:
    - SQLite:  sqlite:///path/to/file.db
    - Postgres: postgresql+psycopg2://user:password@host:5432/dbname
    - MySQL:    mysql+pymysql://user:password@host:3306/dbname
    - SQLServer (ODBC): mssql+pyodbc://user:password@DSNName
    - SQLServer (ODBC string): mssql+pyodbc:///?odbc_connect=<URLENCODED_CONN_STRING>
    """
    return create_engine(conn_str)


def list_tables(engine: Engine, schema: Optional[str] = None, include_views: bool = False) -> List[str]:
    insp = inspect(engine)
    tables = insp.get_table_names(schema=schema)
    if include_views:
        try:
            views = insp.get_view_names(schema=schema)
            tables.extend(views)
        except Exception:
            # Not all dialects support views; ignore if not supported
            pass
    # Deduplicate while preserving order
    seen = set()
    ordered = []
    for t in tables:
        if t not in seen:
            seen.add(t)
            ordered.append(t)
    return ordered


def get_table_columns(engine: Engine, table_name: str, schema: Optional[str] = None) -> List[str]:
    insp = inspect(engine)
    cols = insp.get_columns(table_name, schema=schema)
    return [c["name"] for c in cols]


def get_sample_rows(engine: Engine, table_name: str, schema: Optional[str] = None, sample_rows: int = 5) -> List[Dict[str, Any]]:
    md = MetaData()
    tbl = Table(table_name, md, schema=schema, autoload_with=engine)
    stmt = select(tbl).limit(sample_rows)
    # Use pandas for convenient dict conversion
    df = pd.read_sql(stmt, engine)
    return df.to_dict(orient="records")


def build_tables_and_schema(
    conn_str: str,
    schema: Optional[str] = None,
    sample_rows: int = 5,
    include_views: bool = False,
) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """
    Build a formatted string and a structured dict of tables->columns/sample_data for a DB.

    Returns (tables_and_schema_text, structured_dict) where structured_dict is:
    {
      table_name: {
        "columns": [col1, col2, ...],
        "sample_data": [{...}, ...]
      },
      ...
    }
    """
    engine = get_engine(conn_str)
    tables = list_tables(engine, schema=schema, include_views=include_views)

    structured: Dict[str, Dict[str, Any]] = {}
    parts: List[str] = []

    for t in tables:
        try:
            cols = get_table_columns(engine, t, schema=schema)
        except Exception as e:
            cols = []
        try:
            samples = get_sample_rows(engine, t, schema=schema, sample_rows=sample_rows)
        except Exception:
            samples = []
        structured[t] = {"columns": cols, "sample_data": samples}
        parts.append(f"Table: {t}\n")
        parts.append(f"Columns: {cols}\n")
        parts.append(f"Sample data: {samples}\n")

    return ("".join(parts), structured)

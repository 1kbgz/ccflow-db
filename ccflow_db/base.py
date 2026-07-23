import json
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from ccflow import BaseModel, CallableModel, ContextBase, ContextType, Flow, ResultBase, ResultType
from pydantic import Field

__all__ = (
    "SQLiteCacheStore",
    "SQLiteConfig",
    "SQLiteKeyExistsContext",
    "SQLiteKeyExistsModel",
    "SQLiteKeyExistsResult",
    "SQLiteQueryContext",
    "SQLiteQueryModel",
    "SQLiteQueryResult",
    "SQLiteTableWriteContext",
    "SQLiteTableWriteModel",
    "SQLiteTableWriteResult",
)

SQLiteTableWriteMode = Literal["append", "replace", "upsert"]
SQLiteTableWriteStatus = Literal["written", "replaced", "upserted", "empty"]

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SQLiteConfig(BaseModel):
    path: str | Path = ":memory:"

    def connect(self):
        database_path = str(self.path)
        if database_path != ":memory:":
            Path(database_path).parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(database_path)
        connection.row_factory = sqlite3.Row
        return connection


class SQLiteCacheStore(BaseModel):
    config: SQLiteConfig = Field(default_factory=SQLiteConfig)
    table: str = "cache_entries"

    def _ensure_table(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote_identifier(self.table)} (
                key TEXT PRIMARY KEY,
                payload BLOB NOT NULL,
                content_type TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )

    def uri(self, key: str) -> str:
        return f"sqlite://{self.config.path}/{self.table}/{key}"

    def exists(self, key: str) -> bool:
        with self.config.connect() as connection:
            self._ensure_table(connection)
            row = connection.execute(f"SELECT 1 FROM {_quote_identifier(self.table)} WHERE key = ? LIMIT 1", (key,)).fetchone()
        return row is not None

    def put_bytes(self, key: str, value: bytes, content_type: str | None = None) -> dict[str, Any]:
        with self.config.connect() as connection:
            self._ensure_table(connection)
            connection.execute(
                f"""
                INSERT INTO {_quote_identifier(self.table)} (key, payload, content_type, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    payload = excluded.payload,
                    content_type = excluded.content_type,
                    updated_at = excluded.updated_at
                """,
                (key, sqlite3.Binary(value), content_type, datetime.now(UTC).isoformat()),
            )
        return {"table": self.table, "key": key}

    def get_bytes(self, key: str) -> bytes:
        with self.config.connect() as connection:
            self._ensure_table(connection)
            row = connection.execute(f"SELECT payload FROM {_quote_identifier(self.table)} WHERE key = ? LIMIT 1", (key,)).fetchone()
        if row is None:
            raise FileNotFoundError(self.uri(key))
        return bytes(row["payload"])


class SQLiteQueryContext(ContextBase):
    sql: str
    params: list[Any] | dict[str, Any] = Field(default_factory=list)
    fetch: bool = False


class SQLiteQueryResult(ResultBase):
    rows: list[dict[str, Any]] = Field(default_factory=list)
    rowcount: int


class SQLiteKeyExistsContext(ContextBase):
    table: str
    key: dict[str, Any] = Field(default_factory=dict)


class SQLiteKeyExistsResult(ResultBase):
    table: str
    key: dict[str, Any] = Field(default_factory=dict)
    exists: bool


class SQLiteQueryModel(CallableModel):
    config: SQLiteConfig = Field(default_factory=SQLiteConfig)

    @property
    def context_type(self) -> type[ContextType]:
        return SQLiteQueryContext

    @property
    def result_type(self) -> type[ResultType]:
        return SQLiteQueryResult

    @Flow.call
    def __call__(self, context: SQLiteQueryContext) -> SQLiteQueryResult:
        with self.config.connect() as connection:
            cursor = connection.execute(context.sql, context.params)
            rows = [dict(row) for row in cursor.fetchall()] if context.fetch else []
            return SQLiteQueryResult(rows=rows, rowcount=cursor.rowcount)


class SQLiteTableWriteContext(ContextBase):
    table: str
    rows: list[dict[str, Any]] = Field(default_factory=list)
    mode: SQLiteTableWriteMode = "append"
    primary_key: list[str] = Field(default_factory=list)


class SQLiteTableWriteResult(ResultBase):
    table: str
    status: SQLiteTableWriteStatus
    rows_written: int
    columns: list[str] = Field(default_factory=list)


def _quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Invalid SQLite identifier: {identifier!r}")
    return f'"{identifier}"'


def _columns(rows: list[dict[str, Any]]) -> list[str]:
    columns = []
    for row in rows:
        for column in row:
            if column not in columns:
                columns.append(column)
    return columns


def _sqlite_type(values: list[Any]) -> str:
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            return "INTEGER"
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "REAL"
        if isinstance(value, bytes):
            return "BLOB"
        return "TEXT"
    return "TEXT"


def _column_types(rows: list[dict[str, Any]], columns: list[str]) -> dict[str, str]:
    return {column: _sqlite_type([row.get(column) for row in rows]) for column in columns}


def _sqlite_value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return value


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
    return row is not None


def _create_table_sql(table: str, columns: list[str], column_types: dict[str, str], primary_key: list[str]) -> str:
    column_sql = [f"{_quote_identifier(column)} {column_types[column]}" for column in columns]
    if primary_key:
        missing_keys = [column for column in primary_key if column not in columns]
        if missing_keys:
            raise ValueError(f"Primary key columns are missing from rows: {missing_keys}")
        column_sql.append(f"PRIMARY KEY ({', '.join(_quote_identifier(column) for column in primary_key)})")
    return f"CREATE TABLE IF NOT EXISTS {_quote_identifier(table)} ({', '.join(column_sql)})"


def _insert_sql(table: str, columns: list[str], mode: SQLiteTableWriteMode, primary_key: list[str]) -> str:
    quoted_columns = [_quote_identifier(column) for column in columns]
    placeholders = ", ".join("?" for _column in columns)
    sql = f"INSERT INTO {_quote_identifier(table)} ({', '.join(quoted_columns)}) VALUES ({placeholders})"
    if mode != "upsert":
        return sql
    if not primary_key:
        raise ValueError("SQLite upsert mode requires primary_key.")

    update_columns = [column for column in columns if column not in primary_key]
    if not update_columns:
        return f"{sql} ON CONFLICT({', '.join(_quote_identifier(column) for column in primary_key)}) DO NOTHING"
    assignments = ", ".join(f"{_quote_identifier(column)} = excluded.{_quote_identifier(column)}" for column in update_columns)
    return f"{sql} ON CONFLICT({', '.join(_quote_identifier(column) for column in primary_key)}) DO UPDATE SET {assignments}"


class SQLiteTableWriteModel(CallableModel):
    config: SQLiteConfig = Field(default_factory=SQLiteConfig)

    @property
    def context_type(self) -> type[ContextType]:
        return SQLiteTableWriteContext

    @property
    def result_type(self) -> type[ResultType]:
        return SQLiteTableWriteResult

    @Flow.call
    def __call__(self, context: SQLiteTableWriteContext) -> SQLiteTableWriteResult:
        if not context.rows:
            return SQLiteTableWriteResult(table=context.table, status="empty", rows_written=0, columns=[])

        columns = _columns(context.rows)
        values = [[_sqlite_value(row.get(column)) for column in columns] for row in context.rows]
        column_types = _column_types(context.rows, columns)

        with self.config.connect() as connection:
            if context.mode == "replace":
                connection.execute(f"DROP TABLE IF EXISTS {_quote_identifier(context.table)}")
            connection.execute(_create_table_sql(context.table, columns, column_types, context.primary_key))
            connection.executemany(_insert_sql(context.table, columns, context.mode, context.primary_key), values)

        status: SQLiteTableWriteStatus = {"append": "written", "replace": "replaced", "upsert": "upserted"}[context.mode]
        return SQLiteTableWriteResult(table=context.table, status=status, rows_written=len(context.rows), columns=columns)


class SQLiteKeyExistsModel(CallableModel):
    config: SQLiteConfig = Field(default_factory=SQLiteConfig)

    @property
    def context_type(self) -> type[ContextType]:
        return SQLiteKeyExistsContext

    @property
    def result_type(self) -> type[ResultType]:
        return SQLiteKeyExistsResult

    @Flow.call
    def __call__(self, context: SQLiteKeyExistsContext) -> SQLiteKeyExistsResult:
        if not context.key:
            raise ValueError("SQLite key existence checks require at least one key column.")

        table = _quote_identifier(context.table)
        columns = list(context.key)
        where_clause = " AND ".join(f"{_quote_identifier(column)} = ?" for column in columns)
        values = [_sqlite_value(context.key[column]) for column in columns]

        with self.config.connect() as connection:
            if not _table_exists(connection, context.table):
                return SQLiteKeyExistsResult(table=context.table, key=context.key, exists=False)
            row = connection.execute(f"SELECT 1 FROM {table} WHERE {where_clause} LIMIT 1", values).fetchone()
        return SQLiteKeyExistsResult(table=context.table, key=context.key, exists=row is not None)

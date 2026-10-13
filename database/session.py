from collections.abc import Iterable, Mapping, Sequence
from types import TracebackType
from typing import Any, Self

import polars as pl
import psycopg
from psycopg import Connection, sql
from psycopg.abc import Query

from database.config import DatabaseSettings, settings
from database.writer import copy_dataframe, insert_rows


class PostgresDatabase:
    """
    Manage or reuse a psycopg connection for transactional operations.\
    """

    def __init__(
        self,
        database_settings: DatabaseSettings | None = None,
        connection: Connection[Any] | None = None
    ) -> None:
        """
        Initialize the transactional database facade.

        Args:
            database_settings (DatabaseSettings | None): Connection settings.
                Application settings are used when omitted.
            connection (Connection[Any] | None): Existing connection to
                reuse. A connection is opened lazily when omitted.

        Returns:
            None: The constructor initializes the facade state.
        """
        self.settings = database_settings or settings.database
        self._connection = connection
        self._owns_connection = connection is None

    @property
    def connection(self) -> Connection[Any]:
        """
        Open or return the database connection.

        Returns:
            Connection[Any]: Active PostgreSQL connection managed or reused
                by this facade.
        """
        if self._connection is None:
            self._connection = psycopg.connect(**self.settings.connection_kwargs)
        return self._connection

    def check_connection(self) -> None:
        """
        Verify that PostgreSQL answers a simple query.

        Returns:
            None: Successful completion confirms the connection is usable.
        """
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT 1')
            cursor.fetchone()

    def execute(
        self,
        statement: Query,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> int:
        """
        Execute SQL in the current transaction.

        Args:
            statement (Query): SQL string or composable query.
            parameters (Sequence[Any] | Mapping[str, Any] | None): Positional or
                named values bound to the SQL statement.

        Returns:
            int: Cursor row count reported by PostgreSQL.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(statement, parameters)
            return cursor.rowcount

    def create_schema(self, schema: str) -> None:
        """
        Create a schema when it does not already exist.

        Args:
            schema (str): PostgreSQL schema name.

        Returns:
            None: The schema is created in the current transaction.
        """
        self.execute(
            sql.SQL('CREATE SCHEMA IF NOT EXISTS {}') \
                .format(sql.Identifier(schema))
        )

    def delete(
        self,
        table_name: str,
        schema: str | None = None,
        where: Mapping[str, Any] | None = None
    ) -> int:
        """
        Delete rows using equality filters.

        Args:
            table_name (str): Source table name.
            schema (str | None): Source schema, or ``None`` to use the active
                PostgreSQL search path.
            where (Mapping[str, Any] | None): Column/value equality conditions
                combined with ``AND``.

        Returns:
            int: Number of rows deleted by PostgreSQL.

        Raises:
            ValueError: If no filter condition is provided.
        """
        if not where:
            raise ValueError('delete requires at least one where condition')
        table = (
            sql.Identifier(schema, table_name)
            if schema is not None
            else sql.Identifier(table_name)
        )
        predicates = sql.SQL(' AND ').join(
            sql.SQL('{} = {}').format(sql.Identifier(column), sql.Placeholder())
            for column in where
        )
        statement = sql.SQL('DELETE FROM {} WHERE {}').format(table, predicates)
        return self.execute(statement, tuple(where.values()))

    def truncate(
        self,
        table_name: str,
        schema: str | None = None,
        restart_identity: bool = False,
        cascade: bool = False
    ) -> None:
        """
        Remove every row from a table with optional modifiers.

        Args:
            table_name (str): Table to truncate.
            schema (str | None): Table schema, or ``None`` to use the active
                PostgreSQL search path.
            restart_identity (bool): Whether PostgreSQL sequences should be
                restarted.
            cascade (bool): Whether dependent tables should also be truncated.

        Returns:
            None: The truncate statement is queued in the current transaction.
        """
        table = (
            sql.Identifier(schema, table_name)
            if schema is not None
            else sql.Identifier(table_name)
        )
        statement = sql.SQL('TRUNCATE TABLE {}').format(table)
        if restart_identity:
            statement += sql.SQL(' RESTART IDENTITY')
        if cascade:
            statement += sql.SQL(' CASCADE')
        self.execute(statement)

    def insert(
        self,
        table_name: str,
        rows: Iterable[Mapping[str, Any]],
        schema: str | None = None
    ) -> int:
        """
        Insert row mappings with parameterized SQL.

        Args:
            table_name (str): Destination table name.
            rows (Iterable[Mapping[str, Any]]): Mappings to insert.
            schema (str | None): Destination schema, or ``None`` to use the
                active PostgreSQL search path.

        Returns:
            int: Number of row mappings submitted for insertion.

        Raises:
            ValueError: If the table name or row columns are empty, or row
                column order differs.
        """
        return insert_rows(
            self.connection,
            table_name,
            rows,
            schema=schema
        )

    def upsert(
        self,
        table_name: str,
        rows: Iterable[Mapping[str, Any]],
        conflict_columns: Sequence[str],
        update_columns: Sequence[str] | None = None,
        schema: str | None = None
    ) -> int:
        """
        Insert rows with PostgreSQL conflict handling.

        Args:
            table_name (str): Destination table name.
            rows (Iterable[Mapping[str, Any]]): Mappings to insert or update.
            conflict_columns (Sequence[str]): Columns forming the PostgreSQL
                conflict target.
            update_columns (Sequence[str] | None): Columns updated when a
                conflict occurs. Non-conflict columns are used when omitted.
            schema (str | None): Destination schema, or ``None`` to use the
                active PostgreSQL search path.

        Returns:
            int: Number of row mappings submitted for upsert.

        Raises:
            ValueError: If no conflict column is provided, the table name or
                row columns are empty, row column order differs, or conflict
                or update columns are unknown.
        """
        if not conflict_columns:
            raise ValueError('upsert requires at least one conflict column')
        return insert_rows(
            self.connection,
            table_name,
            rows,
            schema=schema,
            conflict_columns=conflict_columns,
            update_columns=update_columns
        )

    def copy_dataframe(
        self,
        dataframe: pl.DataFrame,
        table_name: str,
        schema: str | None = None,
        columns: Sequence[str] | None = None
    ) -> int:
        """
        Load a DataFrame using PostgreSQL ``COPY FROM STDIN``.

        Args:
            dataframe (pl.DataFrame): Rows to load.
            table_name (str): Destination table name.
            schema (str | None): Destination schema, or ``None`` to use the
                active PostgreSQL search path.
            columns (Sequence[str] | None): Ordered columns to copy. All
                DataFrame columns are used when omitted.

        Returns:
            int: Number of DataFrame rows sent through COPY.

        Raises:
            ValueError: If the table or schema name is empty, or a selected
                column does not exist in the DataFrame.
        """
        return copy_dataframe(
            self.connection,
            dataframe,
            table_name,
            schema=schema,
            columns=columns
        )

    def close(self) -> None:
        """
        Close the connection when it is owned by this facade.

        Returns:
            None: The owned connection is closed and cleared.
        """
        if self._connection is not None and self._owns_connection:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> Self:
        """
        Open the managed context.

        Returns:
            Self: This database facade with an active connection.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None
    ) -> None:
        """
        Commit or roll back an owned connection and then close it.

        Args:
            exc_type (type[BaseException] | None): Exception type raised inside
                the context, when present.
            exc_value (BaseException | None): Exception instance raised inside
                the context, when present.
            traceback (TracebackType | None): Exception traceback, when present.

        Returns:
            None: Context cleanup does not suppress exceptions.
        """
        if self._connection is None or not self._owns_connection:
            return
        try:
            if exc_type is None:
                self._connection.commit()
            else:
                self._connection.rollback()
        finally:
            self.close()

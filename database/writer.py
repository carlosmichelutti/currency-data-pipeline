from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pandas as pd
from psycopg import Connection, sql


def copy_dataframe(
    connection: Connection[Any],
    dataframe: pd.DataFrame,
    table_name: str,
    schema: str | None = None,
    columns: Sequence[str] | None = None
) -> int:
    """
    Load a DataFrame through PostgreSQL's row-oriented ``COPY`` protocol.

    Args:
        connection (Connection[Any]): Active connection that owns the COPY
            transaction.
        dataframe (pd.DataFrame): Rows to load into PostgreSQL.
        table_name (str): Destination table name.
        schema (str | None): Destination schema, or ``None`` to use the active
            PostgreSQL search path.
        columns (Sequence[str] | None): Ordered DataFrame columns to copy. All
            columns are used when omitted.

    Returns:
        int: Number of DataFrame rows sent through COPY.

    Raises:
        ValueError: If the table or schema name is empty, or a selected column
            does not exist in the DataFrame.
    """
    _validate_identifier(table_name, 'table_name')
    if schema is not None:
        _validate_identifier(schema, 'schema')

    selected_columns = list(columns or dataframe.columns)
    if not selected_columns:
        return 0
    if missing := set(selected_columns).difference(dataframe.columns):
        raise ValueError(f'DataFrame does not contain columns: {sorted(missing)}')

    table = (
        sql.Identifier(schema, table_name)
        if schema is not None
        else sql.Identifier(table_name)
    )
    statement = sql.SQL('COPY {} ({}) FROM STDIN').format(
        table,
        sql.SQL(', ').join(map(sql.Identifier, selected_columns))
    )

    with connection.cursor() as cursor, cursor.copy(statement) as copy:
        for row in dataframe.loc[:, selected_columns].itertuples(
            index=False,
            name=None
        ):
            copy.write_row(tuple(_to_python(value) for value in row))

    return len(dataframe)


def insert_rows(
    connection: Connection[Any],
    table_name: str,
    rows: Iterable[Mapping[str, Any]],
    schema: str | None = None,
    conflict_columns: Sequence[str] | None = None,
    update_columns: Sequence[str] | None = None
) -> int:
    """
    Insert mappings, optionally handling conflicts with PostgreSQL.

    Args:
        connection (Connection[Any]): Active connection that owns the
            insert transaction.
        table_name (str): Destination table name.
        rows (Iterable[Mapping[str, Any]]): Row mappings with a consistent key
            order.
        schema (str | None): Destination schema, or ``None`` to use the active
            PostgreSQL search path.
        conflict_columns (Sequence[str] | None): Columns forming the conflict
            target. When omitted, a regular INSERT is issued.
        update_columns (Sequence[str] | None): Columns updated from EXCLUDED on
            conflict. All non-conflict columns are used when omitted.

    Returns:
        int: Number of row mappings submitted for insertion.

    Raises:
        ValueError: If the table name or row columns are empty, row column
            order differs, or conflict or update columns are unknown.
    """
    _validate_identifier(table_name, 'table_name')
    materialized_rows = list(rows)
    if not materialized_rows:
        return 0

    columns = list(materialized_rows[0])
    if not columns:
        raise ValueError('rows must contain at least one column')
    if any(list(row) != columns for row in materialized_rows):
        raise ValueError('all rows must have the same columns in the same order')

    table = (
        sql.Identifier(schema, table_name)
        if schema is not None
        else sql.Identifier(table_name)
    )
    placeholders = sql.SQL(', ').join(sql.Placeholder() for _ in columns)
    statement = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
        table,
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        placeholders
    )

    if conflict_columns:
        unknown_conflicts = set(conflict_columns).difference(columns)
        if unknown_conflicts:
            raise ValueError(f'unknown conflict columns: {sorted(unknown_conflicts)}')
        conflict_target = sql.SQL(', ').join(map(sql.Identifier, conflict_columns))
        resolved_updates = list(
            update_columns
            if update_columns is not None
            else (column for column in columns if column not in conflict_columns)
        )
        unknown_updates = set(resolved_updates).difference(columns)
        if unknown_updates:
            raise ValueError(f'unknown update columns: {sorted(unknown_updates)}')
        if resolved_updates:
            assignments = sql.SQL(', ').join(
                sql.SQL('{} = EXCLUDED.{}').format(
                    sql.Identifier(column),
                    sql.Identifier(column)
                )
                for column in resolved_updates
            )
            statement += sql.SQL(' ON CONFLICT ({}) DO UPDATE SET {}').format(
                conflict_target,
                assignments
            )
        else:
            statement += sql.SQL(' ON CONFLICT ({}) DO NOTHING').format(
                conflict_target
            )

    parameters = [
        tuple(_to_python(row[column]) for column in columns)
        for row in materialized_rows
    ]
    with connection.cursor() as cursor:
        cursor.executemany(statement, parameters)
    return len(materialized_rows)


def _to_python(value: Any) -> Any:
    """
    Convert a pandas or numpy scalar into an object psycopg can adapt.

    Args:
        value (Any): Scalar value read from a DataFrame or row mapping.

    Returns:
        Any: Native Python scalar, datetime, ``None``, or the original value.
    """
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    item = getattr(value, 'item', None)
    return item() if callable(item) else value


def _validate_identifier(value: str, field_name: str) -> None:
    """
    Reject an empty SQL identifier.

    Args:
        value (str): Identifier value to validate.
        field_name (str): Logical field name included in an error message.

    Returns:
        None: The function only validates the supplied value.

    Raises:
        ValueError: If the identifier is empty or contains only whitespace.
    """
    if not value or not value.strip():
        raise ValueError(f'{field_name} cannot be empty')

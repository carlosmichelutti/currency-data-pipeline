from pathlib import Path

from database.session import PostgresDatabase


def execute_sql_directory(
    database: PostgresDatabase,
    directory: str | Path
) -> list[Path]:
    """
    Execute every non-empty ``.sql`` file below a directory in name order.

    Args:
        database (PostgresDatabase): Active transactional database facade used
            to execute each SQL document.
        directory (str | Path): Directory containing the layer's SQL
            files. Nested directories are supported.

    Returns:
        list[Path]: Ordered absolute paths of the SQL files executed.

    Raises:
        FileNotFoundError: If the directory does not exist or contains no SQL
            files.
    """

    sql_directory = Path(directory).resolve()
    if not sql_directory.is_dir():
        raise FileNotFoundError(f'SQL directory does not exist: {sql_directory}')

    sql_files = sorted(
        path for path in sql_directory.rglob('*.sql') if path.is_file()
    )
    if not sql_files:
        raise FileNotFoundError(f'No SQL files found below: {sql_directory}')

    executed_files: list[Path] = []
    for sql_file in sql_files:
        statement = sql_file.read_text(encoding='utf-8').strip()
        if statement:
            database.execute(statement)
            executed_files.append(sql_file)
    return executed_files

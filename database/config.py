from pathlib import Path
from urllib.parse import quote_plus

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT_DIR / '.env'


class DatabaseSettings(BaseSettings):
    """
    Load and validate ``DATABASE_*`` environment variables.

    Args:
        host (str): PostgreSQL server hostname or address.
        port (int): PostgreSQL TCP port, defaulting to 5432.
        name (str): Name of the database to connect to.
        user (str): PostgreSQL username.
        password (SecretStr): PostgreSQL password kept as a secret value.
    """
    host: str
    port: int = 5432
    name: str
    user: str
    password: SecretStr

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding='utf-8',
        env_prefix='DATABASE_',
        extra='ignore'
    )

    @field_validator('host', 'name', 'user')
    @classmethod
    def reject_blank_values(cls, value: str) -> str:
        """
        Strip a textual database setting and reject an empty value.

        Args:
            value (str): Database setting supplied by Pydantic.

        Returns:
            str: Normalized non-empty setting.

        Raises:
            ValueError: If the setting is empty after surrounding whitespace is
                removed.
        """
        normalized = value.strip()
        if not normalized:
            raise ValueError('database settings cannot be blank')
        return normalized

    @field_validator('port')
    @classmethod
    def validate_port(cls, value: int) -> int:
        """
        Ensure the configured TCP port is valid.

        Args:
            value (int): TCP port supplied by Pydantic.

        Returns:
            int: Validated TCP port.

        Raises:
            ValueError: If the port is outside the valid TCP range.
        """
        if not 1 <= value <= 65535:
            raise ValueError('database port must be between 1 and 65535')
        return value

    @property
    def database_url(self) -> str:
        """
        Build a PostgreSQL URL with encoded credentials.

        Returns:
            str: PostgreSQL URL suitable for display or URL-based tooling.
        """
        user = quote_plus(self.user)
        password = quote_plus(self.password.get_secret_value())
        return f'postgresql://{user}:{password}@{self.host}:{self.port}/{self.name}'

    @property
    def connection_kwargs(self) -> dict[str, str | int]:
        """
        Build raw keyword arguments expected by :func:`psycopg.connect`.

        Returns:
            dict[str, str | int]: Host, port, datbaase name, username, and raw
                password used to create a psycopg connection.
        """
        return {
            'host': self.host,
            'port': self.port,
            'dbname': self.name,
            'user': self.user,
            'password': self.password.get_secret_value()
        }


class Settings(BaseModel):
    """
    Group application settings by concern.

    Args:
        database (DatabaseSettings): Database connection settings created from
            the environment when omitted.
    """
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)


settings = Settings()

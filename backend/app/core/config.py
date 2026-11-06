from pydantic import AnyHttpUrl, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")

    postgres_db: str = Field(min_length=1)
    postgres_user: str = Field(min_length=1)
    postgres_password: SecretStr = Field(min_length=1)
    postgres_host: str = Field(default="postgres", min_length=1)
    postgres_port: int = Field(default=5432, ge=1, le=65535)

    def database_url(self) -> URL:
        return URL.create(
            "postgresql+psycopg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        )


class FetchSettings(BaseSettings):
    """Network settings are separate so pure fetcher tests need no database secrets."""

    model_config = SettingsConfigDict(
        case_sensitive=False, extra="ignore", hide_input_in_errors=True
    )

    crawl_contact_email: SecretStr = SecretStr("contact@example.invalid")
    crawl_timeout_s: float = Field(default=15.0, gt=0, allow_inf_nan=False)
    crawl_total_timeout_s: float = Field(default=60, gt=0, le=3600, allow_inf_nan=False)
    crawl_delay_s: float = Field(default=2.0, gt=0, allow_inf_nan=False)
    crawl_max_retries: int = Field(default=2, ge=0, le=10)
    crawl_backoff_s: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    crawl_retry_after_cap_s: float = Field(default=60.0, gt=0, allow_inf_nan=False)
    crawl_max_redirects: int = Field(default=5, ge=0, le=20)
    crawl_max_response_bytes: int = Field(default=10 * 1024 * 1024, gt=0)
    robots_cache_ttl_s: float = Field(default=3600.0, gt=0, allow_inf_nan=False)

    @field_validator("crawl_contact_email")
    @classmethod
    def validate_contact(cls, value: SecretStr) -> SecretStr:
        email = value.get_secret_value()
        if (
            email.count("@") != 1
            or not all(email.split("@"))
            or not email.isascii()
            or not email.isprintable()
            or any(
                character.isspace() or character in "();<>" or ord(character) < 33
                for character in email
            )
        ):
            raise ValueError("Contact must be a plain email address")
        return value

    def user_agent(self) -> str:
        return f"RegWatch/0.1 (+mailto:{self.crawl_contact_email.get_secret_value()})"


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False, extra="ignore", hide_input_in_errors=True
    )

    minio_root_user: SecretStr = Field(min_length=1)
    minio_root_password: SecretStr = Field(min_length=1)
    snapshot_bucket: str = Field(
        default="regwatch-snapshots", pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"
    )
    s3_endpoint_url: AnyHttpUrl = AnyHttpUrl("http://minio:9000")
    s3_region: str = Field(default="us-east-1", min_length=1)
    s3_connect_timeout_s: float = Field(default=3, gt=0, allow_inf_nan=False)
    s3_read_timeout_s: float = Field(default=10, gt=0, allow_inf_nan=False)
    s3_max_attempts: int = Field(default=3, ge=1, le=10)

    @field_validator("s3_endpoint_url")
    @classmethod
    def validate_endpoint(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username or value.password or value.query or value.fragment or value.path != "/":
            raise ValueError("S3 endpoint must be an HTTP(S) origin without credentials")
        return value


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False, extra="ignore", hide_input_in_errors=True
    )

    redis_host: str = Field(default="redis", min_length=1)
    redis_port: int = Field(default=6379, ge=1, le=65535)
    redis_db: int = Field(default=0, ge=0, le=15)
    redis_password: SecretStr | None = None
    redis_timeout_s: float = Field(default=2, gt=0, le=30, allow_inf_nan=False)
    redis_max_connections: int = Field(default=50, ge=1, le=1000)
    redis_key_prefix: str = Field(default="regwatch", pattern=r"^[a-zA-Z0-9:_-]{1,80}$")
    rate_interval_s: float = Field(default=5, ge=0.001, le=3600, allow_inf_nan=False)
    rate_capacity: int = Field(default=1, ge=1, le=100)

    domain_concurrency: int = Field(default=1, ge=1, le=100)
    coordination_wait_s: float = Field(default=120, gt=0, le=3600, allow_inf_nan=False)

from pydantic import Field, SecretStr, field_validator
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

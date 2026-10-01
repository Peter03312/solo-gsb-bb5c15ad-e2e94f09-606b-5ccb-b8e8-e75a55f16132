from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, overridable via environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="CAL_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    app_name: str = Field(default="calibration-timeline-service")
    # Host/port the container listens on (documented in README and Compose).
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8080)
    log_level: str = Field(default="info")
    # Storage backend:
    #   memory - process-local state, lost on restart (the original behavior);
    #   sqlite - durable, multi-process state in one WAL-mode SQLite file.
    storage_backend: Literal["memory", "sqlite"] = Field(default="memory")
    # Database file for storage_backend="sqlite". Parent directories are
    # created automatically; mount a volume here in Compose so state survives
    # restarts and is shared by every service process.
    sqlite_path: str = Field(default="/data/calibration.db")
    # Milliseconds a write waits for another process' SQLite lock before the
    # request fails with an explicit storage error (503).
    sqlite_busy_timeout_ms: int = Field(default=5000, ge=0)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

StorageBackend = Literal["memory", "sqlite"]


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
    # Where revisions, idempotency keys and sealed evidence live:
    #   "sqlite" - durable SQLite file, kept across restarts and shared by
    #              every service process using the same CAL_SQLITE_PATH;
    #   "memory" - process-local, lost on restart/reset (legacy behavior).
    storage: StorageBackend = Field(default="sqlite")
    # SQLite database file used when storage="sqlite". The parent directory
    # is created on startup; WAL mode additionally creates -wal/-shm files
    # next to it. ":memory:" is rejected (not shared/durable).
    sqlite_path: str = Field(default="data/calibration.db")
    # Milliseconds a writer waits on another process holding the SQLite lock
    # before reporting a storage error.
    sqlite_busy_timeout_ms: int = Field(default=10000, ge=1)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

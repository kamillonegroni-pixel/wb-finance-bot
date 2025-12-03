"""Project configuration helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    """Simple settings object backed by environment variables."""

    api_base_url: str = os.environ.get(
        "WB_API_BASE_URL", "https://statistics-api.wildberries.ru"
    )
    api_token: str = os.environ.get("WB_API_TOKEN", "")
    db_path: str = os.environ.get("WB_DB_PATH", os.path.join("db", "wb_reports.db"))
    schema_path: str = os.environ.get("WB_SCHEMA_PATH", os.path.join("db", "schema.sql"))
    page_limit: int = int(os.environ.get("WB_PAGE_LIMIT", "1000"))
    request_timeout: int = int(os.environ.get("WB_REQUEST_TIMEOUT", "30"))

    def require_token(self) -> "Settings":
        """Ensure WB token present before hitting the API."""

        if not self.api_token:
            raise RuntimeError(
                "WB_API_TOKEN environment variable is required to call Wildberries API"
            )
        return self


settings = Settings()

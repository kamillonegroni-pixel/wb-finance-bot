"""Wildberries API client for financial reports."""
from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Optional

import requests

logger = logging.getLogger(__name__)


class WbApiClient:
    """Minimal Wildberries statistics API client."""

    def __init__(
        self,
        token: str,
        base_url: str = "https://statistics-api.wildberries.ru",
        timeout: int = 30,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not token:
            raise ValueError("API token is required")
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = session or requests.Session()

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": self._token,
            "Accept": "application/json",
        }

    def _request_rrd_page(
        self,
        date_from: str,
        date_to: str,
        rrd_id: int,
        limit: int,
    ) -> List[Dict]:
        endpoint = f"{self._base_url}/api/v1/supplier/reportDetailByPeriod"
        params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "rrdid": rrd_id,
            "limit": limit,
        }
        response = self._session.get(
            endpoint,
            params=params,
            headers=self.headers,
            timeout=self._timeout,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError("Unexpected response format: expected list of records")
        return data

    def iter_report_detail(
        self,
        date_from: str,
        date_to: str,
        limit: int = 1000,
    ) -> Iterator[Dict]:
        """Iterate over full RRD dataset with automatic pagination."""

        next_rrd_id = 0
        while True:
            page = self._request_rrd_page(date_from, date_to, next_rrd_id, limit)
            if not page:
                logger.debug("RRD pagination finished at id=%s", next_rrd_id)
                break

            for row in page:
                yield row

            last_rrd = page[-1].get("rrd_id")
            if last_rrd is None or last_rrd == next_rrd_id:
                # Prevent infinite loops if API stops providing new IDs
                logger.debug("Stopping pagination due to missing/unchanged rrd_id")
                break

            next_rrd_id = last_rrd

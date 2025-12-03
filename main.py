"""Entry point for downloading and storing Wildberries RRD reports."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

import requests

from api.wb_api_client import WbApiClient
from config import settings
from db.sqlite_manager import SQLiteManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Wildberries RRD data")
    parser.add_argument(
        "--date-from",
        required=True,
        help="Start date in YYYY-MM-DD",
    )
    parser.add_argument(
        "--date-to",
        required=True,
        help="End date in YYYY-MM-DD",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Page size for API pagination (defaults to config).",
    )
    return parser.parse_args()


def validate_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid date format '{value}'. Use YYYY-MM-DD") from exc
    return value


def main() -> None:
    args = parse_args()
    date_from = validate_date(args.date_from)
    date_to = validate_date(args.date_to)

    cfg = settings.require_token()
    page_limit = args.limit or cfg.page_limit

    client = WbApiClient(
        token=cfg.api_token,
        base_url=cfg.api_base_url,
        timeout=cfg.request_timeout,
    )

    try:
        with SQLiteManager(cfg.db_path, cfg.schema_path) as manager:
            unique_count = manager.upsert_many(
                client.iter_report_detail(date_from, date_to, page_limit)
            )
    except requests.HTTPError as http_err:
        raise SystemExit(f"API request failed: {http_err}") from http_err
    except requests.RequestException as req_err:
        raise SystemExit(f"Network error: {req_err}") from req_err

    print(f"Loaded {unique_count} unique RRD rows into {cfg.db_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("Interrupted by user")

"""SQLite helpers for storing Wildberries reports."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Sequence

NUMERIC_SUM_COLUMNS = (
    "quantity",
    "retail_price",
    "retail_amount",
    "ppvz_for_pay",
    "delivery_rub",
    "logistics",
    "wb_commission",
    "return_amount",
    "penalty",
)

TEXT_COLUMNS = (
    "article",
    "barcode",
    "subject",
    "brand",
    "sa_name",
    "tech_size",
    "date",
)

ID_COLUMNS = ("rrd_id", "nm_id", "income_id", "sale_id")

ALL_COLUMNS = (
    "rrd_id",
    "nm_id",
    "article",
    "barcode",
    "subject",
    "brand",
    "sa_name",
    "tech_size",
    "income_id",
    "sale_id",
    "quantity",
    "retail_price",
    "retail_amount",
    "ppvz_for_pay",
    "delivery_rub",
    "logistics",
    "wb_commission",
    "return_amount",
    "penalty",
    "date",
    "cost_price",
)

UPSERT_SQL = """
INSERT INTO wb_rrd (
    rrd_id, nm_id, article, barcode, subject, brand, sa_name, tech_size,
    income_id, sale_id, quantity, retail_price, retail_amount, ppvz_for_pay,
    delivery_rub, logistics, wb_commission, return_amount, penalty, date, cost_price
) VALUES (
    :rrd_id, :nm_id, :article, :barcode, :subject, :brand, :sa_name, :tech_size,
    :income_id, :sale_id, :quantity, :retail_price, :retail_amount, :ppvz_for_pay,
    :delivery_rub, :logistics, :wb_commission, :return_amount, :penalty, :date, :cost_price
)
ON CONFLICT(rrd_id) DO UPDATE SET
    nm_id = excluded.nm_id,
    article = COALESCE(excluded.article, wb_rrd.article),
    barcode = COALESCE(excluded.barcode, wb_rrd.barcode),
    subject = COALESCE(excluded.subject, wb_rrd.subject),
    brand = COALESCE(excluded.brand, wb_rrd.brand),
    sa_name = COALESCE(excluded.sa_name, wb_rrd.sa_name),
    tech_size = COALESCE(excluded.tech_size, wb_rrd.tech_size),
    income_id = COALESCE(excluded.income_id, wb_rrd.income_id),
    sale_id = COALESCE(excluded.sale_id, wb_rrd.sale_id),
    quantity = COALESCE(wb_rrd.quantity, 0) + COALESCE(excluded.quantity, 0),
    retail_price = COALESCE(wb_rrd.retail_price, 0) + COALESCE(excluded.retail_price, 0),
    retail_amount = COALESCE(wb_rrd.retail_amount, 0) + COALESCE(excluded.retail_amount, 0),
    ppvz_for_pay = COALESCE(wb_rrd.ppvz_for_pay, 0) + COALESCE(excluded.ppvz_for_pay, 0),
    delivery_rub = COALESCE(wb_rrd.delivery_rub, 0) + COALESCE(excluded.delivery_rub, 0),
    logistics = COALESCE(wb_rrd.logistics, 0) + COALESCE(excluded.logistics, 0),
    wb_commission = COALESCE(wb_rrd.wb_commission, 0) + COALESCE(excluded.wb_commission, 0),
    return_amount = COALESCE(wb_rrd.return_amount, 0) + COALESCE(excluded.return_amount, 0),
    penalty = COALESCE(wb_rrd.penalty, 0) + COALESCE(excluded.penalty, 0),
    date = COALESCE(excluded.date, wb_rrd.date),
    cost_price = wb_rrd.cost_price
"""


class SQLiteManager:
    """Connection wrapper with simple upsert helpers."""

    def __init__(self, db_path: str, schema_path: str) -> None:
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        schema_sql = self.schema_path.read_text(encoding="utf-8")
        self.conn.executescript(schema_sql)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "SQLiteManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def upsert_many(self, records: Iterable[Dict]) -> int:
        unique_ids = set()
        with self.conn:
            for record in records:
                normalized = self._normalize_record(record)
                if normalized["rrd_id"] is None:
                    continue
                unique_ids.add(normalized["rrd_id"])
                self.conn.execute(UPSERT_SQL, normalized)
        return len(unique_ids)

    def _normalize_record(self, raw: Dict) -> Dict:
        normalized: Dict = {key: None for key in ALL_COLUMNS}

        for column in TEXT_COLUMNS:
            normalized[column] = raw.get(column)

        normalized["rrd_id"] = _to_int(raw.get("rrd_id"))
        normalized["nm_id"] = _to_int(raw.get("nm_id"))
        normalized["income_id"] = _to_int(raw.get("income_id"))
        normalized["sale_id"] = _to_int(raw.get("sale_id"))

        for column in NUMERIC_SUM_COLUMNS:
            normalized[column] = _to_float(raw.get(column))

        normalized["cost_price"] = None  # intentionally empty per requirements
        return normalized


def _to_float(value) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

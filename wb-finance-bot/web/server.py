"""FastAPI server exposing recent Wildberries RRD rows."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Absolute path to the SQLite DB file used by the API.
# We keep it as a Path object to safely work with filesystem checks.
DB_PATH = (
    Path(__file__)
    .resolve()
    .parent.parent  # project root
    / "db"
    / "wb_reports.db"
)

app = FastAPI(title="WB Financial Reports")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def fetch_latest_rrd(limit: int = 100) -> List[dict]:
    if not DB_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Database not found at {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT rrd_id, nm_id, article, barcode, subject,
                   retail_amount, ppvz_for_pay, delivery_rub,
                   logistics, date
            FROM wb_rrd
            ORDER BY date DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


@app.get("/rrd")
async def get_rrd(limit: int = 100) -> List[dict]:
    """Return the latest RRD rows ordered by date."""

    return fetch_latest_rrd(limit=limit)

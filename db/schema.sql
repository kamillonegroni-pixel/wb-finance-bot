CREATE TABLE IF NOT EXISTS wb_rrd (
    rrd_id INTEGER PRIMARY KEY,
    nm_id INTEGER,
    article TEXT,
    barcode TEXT,
    subject TEXT,
    brand TEXT,
    sa_name TEXT,
    tech_size TEXT,
    income_id INTEGER,
    sale_id INTEGER,
    quantity REAL DEFAULT 0,
    retail_price REAL DEFAULT 0,
    retail_amount REAL DEFAULT 0,
    ppvz_for_pay REAL DEFAULT 0,
    delivery_rub REAL DEFAULT 0,
    logistics REAL DEFAULT 0,
    wb_commission REAL DEFAULT 0,
    return_amount REAL DEFAULT 0,
    penalty REAL DEFAULT 0,
    date TEXT,
    cost_price REAL
);

CREATE INDEX IF NOT EXISTS idx_wb_rrd_date ON wb_rrd(date);

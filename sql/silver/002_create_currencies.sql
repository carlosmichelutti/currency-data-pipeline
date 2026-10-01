CREATE TABLE IF NOT EXISTS silver.currencies (
    iso_code TEXT NOT NULL CHECK (iso_code ~ '^[A-Z]{3}$'),
    iso_numeric TEXT CHECK (iso_numeric ~ '^[0-9]{3}$'),
    name TEXT NOT NULL CHECK (name <> ''),
    symbol TEXT,
    start_date DATE,
    end_date DATE,
    _source_path TEXT NOT NULL,
    _extension_file TEXT NOT NULL,
    _number_row BIGINT NOT NULL,
    _batch_id TEXT NOT NULL,
    _ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (_batch_id, _number_row),
    CHECK (start_date <= end_date)
);

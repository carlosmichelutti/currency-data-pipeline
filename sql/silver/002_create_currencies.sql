CREATE TABLE IF NOT EXISTS silver.currencies (
    iso_code TEXT NOT NULL CHECK (iso_code ~ '^[A-Z]{3}$'),
    iso_numeric TEXT CHECK (iso_numeric ~ '^[0-9]{3}$'),
    name TEXT NOT NULL CHECK (name <> ''),
    symbol TEXT,
    start_date DATE,
    end_date DATE,
    _source_path TEXT NOT NULL,
    _source_file TEXT NOT NULL,
    _source_extension TEXT NOT NULL,
    _source_row_number BIGINT NOT NULL,
    _ingestion_batch_id TEXT NOT NULL,
    _ingested_at_utc TIMESTAMPTZ NOT NULL,
    _record_hash TEXT NOT NULL,
    PRIMARY KEY (_ingestion_batch_id, _source_row_number),
    CHECK (start_date <= end_date)
);

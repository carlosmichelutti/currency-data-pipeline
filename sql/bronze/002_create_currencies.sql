CREATE TABLE IF NOT EXISTS bronze.currencies (
    iso_code TEXT,
    iso_numeric TEXT,
    name TEXT,
    symbol TEXT,
    start_date TEXT,
    end_date TEXT,
    _source_path TEXT NOT NULL,
    _source_file TEXT NOT NULL,
    _source_extension TEXT NOT NULL,
    _source_row_number BIGINT NOT NULL,
    _ingestion_batch_id TEXT NOT NULL,
    _ingested_at_utc TIMESTAMPTZ NOT NULL,
    _record_hash TEXT NOT NULL,
    PRIMARY KEY (_ingestion_batch_id, _source_row_number)
);

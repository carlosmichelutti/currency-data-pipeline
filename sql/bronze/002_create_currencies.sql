CREATE TABLE IF NOT EXISTS bronze.currencies (
    iso_code TEXT,
    iso_numeric TEXT,
    name TEXT,
    symbol TEXT,
    start_date TEXT,
    end_date TEXT,
    _source_path TEXT NOT NULL,
    _extension_file TEXT NOT NULL,
    _number_row BIGINT NOT NULL,
    _batch_id TEXT NOT NULL,
    _ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (_batch_id, _number_row)
);

CREATE TABLE IF NOT EXISTS bronze.rates (
    date TEXT,
    base TEXT,
    quote TEXT,
    rate TEXT,
    _source_path TEXT NOT NULL,
    _extension_file TEXT NOT NULL,
    _number_row BIGINT NOT NULL,
    _batch_id TEXT NOT NULL,
    _ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (_batch_id, _number_row)
);

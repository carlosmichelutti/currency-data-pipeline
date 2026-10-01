CREATE TABLE IF NOT EXISTS silver.rates (
    date DATE NOT NULL,
    base TEXT NOT NULL CHECK (base ~ '^[A-Z]{3}$'),
    quote TEXT NOT NULL CHECK (quote ~ '^[A-Z]{3}$'),
    rate DOUBLE PRECISION NOT NULL
        CHECK (rate > 0 AND rate < 'Infinity'::DOUBLE PRECISION),
    _source_path TEXT NOT NULL,
    _source_file TEXT NOT NULL,
    _source_extension TEXT NOT NULL,
    _source_row_number BIGINT NOT NULL,
    _ingestion_batch_id TEXT NOT NULL,
    _ingested_at_utc TIMESTAMPTZ NOT NULL,
    _record_hash TEXT NOT NULL,
    PRIMARY KEY (_ingestion_batch_id, _source_row_number)
);

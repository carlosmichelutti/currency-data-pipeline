CREATE TABLE IF NOT EXISTS silver.rates (
    date DATE NOT NULL,
    base TEXT NOT NULL CHECK (base ~ '^[A-Z]{3}$'),
    quote TEXT NOT NULL CHECK (quote ~ '^[A-Z]{3}$'),
    rate DOUBLE PRECISION NOT NULL
        CHECK (rate > 0 AND rate < 'Infinity'::DOUBLE PRECISION),
    _source_path TEXT NOT NULL,
    _extension_file TEXT NOT NULL,
    _number_row BIGINT NOT NULL,
    _batch_id TEXT NOT NULL,
    _ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (_batch_id, _number_row)
);

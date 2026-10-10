CREATE TABLE IF NOT EXISTS gold.dim_currency (
    currency_key INTEGER PRIMARY KEY CHECK (currency_key BETWEEN 1 AND 17576),
    iso_code TEXT NOT NULL UNIQUE CHECK (iso_code ~ '^[A-Z]{3}$'),
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
    CHECK (start_date <= end_date),
    CHECK (
        currency_key = (ascii(substr(iso_code, 1, 1)) - 65) * 676
                     + (ascii(substr(iso_code, 2, 1)) - 65) * 26
                     + (ascii(substr(iso_code, 3, 1)) - 65) + 1
    )
);

COMMENT ON TABLE gold.dim_currency IS
    'One member per ISO code; attributes are updated in place (SCD type 1). Shared by base and quote roles.';
COMMENT ON COLUMN gold.dim_currency.currency_key IS
    'Stable collision-free base-26 encoding of the ISO code (AAA=1, ZZZ=17576).';
COMMENT ON COLUMN gold.dim_currency.start_date IS
    'Start of publication coverage in the source API; not SCD validity.';
COMMENT ON COLUMN gold.dim_currency.end_date IS
    'End of publication coverage in the source API; not SCD validity.';

-- Apply the metadata-free model to existing gold tables without removing rows.
DROP INDEX IF EXISTS gold.ix_exchange_rate_batch;

ALTER TABLE gold.dim_currency
    DROP COLUMN IF EXISTS _source_path,
    DROP COLUMN IF EXISTS _source_file,
    DROP COLUMN IF EXISTS _source_extension,
    DROP COLUMN IF EXISTS _source_row_number,
    DROP COLUMN IF EXISTS _ingestion_batch_id,
    DROP COLUMN IF EXISTS _ingested_at_utc,
    DROP COLUMN IF EXISTS _record_hash;

ALTER TABLE gold.fact_exchange_rate
    DROP COLUMN IF EXISTS _source_path,
    DROP COLUMN IF EXISTS _source_file,
    DROP COLUMN IF EXISTS _source_extension,
    DROP COLUMN IF EXISTS _source_row_number,
    DROP COLUMN IF EXISTS _ingestion_batch_id,
    DROP COLUMN IF EXISTS _ingested_at_utc,
    DROP COLUMN IF EXISTS _record_hash;

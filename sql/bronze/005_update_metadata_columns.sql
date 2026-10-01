DO $$
DECLARE
    target_table TEXT;
BEGIN
    FOREACH target_table IN ARRAY ARRAY['currencies', 'rates']
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_extension_file'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_source_extension'
        ) THEN
            EXECUTE format(
                'ALTER TABLE bronze.%I RENAME COLUMN _extension_file TO _source_extension',
                target_table
            );
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_number_row'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_source_row_number'
        ) THEN
            EXECUTE format(
                'ALTER TABLE bronze.%I RENAME COLUMN _number_row TO _source_row_number',
                target_table
            );
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_batch_id'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_ingestion_batch_id'
        ) THEN
            EXECUTE format(
                'ALTER TABLE bronze.%I RENAME COLUMN _batch_id TO _ingestion_batch_id',
                target_table
            );
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_ingested_at'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_ingested_at_utc'
        ) THEN
            EXECUTE format(
                'ALTER TABLE bronze.%I RENAME COLUMN _ingested_at TO _ingested_at_utc',
                target_table
            );
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_row_hash'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = target_table
              AND column_name = '_record_hash'
        ) THEN
            EXECUTE format(
                'ALTER TABLE bronze.%I RENAME COLUMN _row_hash TO _record_hash',
                target_table
            );
        END IF;

        EXECUTE format(
            'ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS _source_file TEXT',
            target_table
        );
        EXECUTE format(
            'ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS _record_hash TEXT',
            target_table
        );
        EXECUTE format(
            'UPDATE bronze.%I SET _source_file = regexp_replace(_source_path, ''^.*/'', '''') WHERE _source_file IS NULL',
            target_table
        );
        EXECUTE format(
            'ALTER TABLE bronze.%I ALTER COLUMN _source_file SET NOT NULL',
            target_table
        );
    END LOOP;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'bronze'
          AND table_name = 'currencies'
          AND column_name IN ('start_date', 'end_date')
          AND data_type <> 'text'
    ) THEN
        ALTER TABLE bronze.currencies
            ALTER COLUMN start_date TYPE TEXT USING start_date::TEXT,
            ALTER COLUMN end_date TYPE TEXT USING end_date::TEXT;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'bronze'
          AND table_name = 'rates'
          AND column_name IN ('date', 'rate')
          AND data_type <> 'text'
    ) THEN
        ALTER TABLE bronze.rates
            ALTER COLUMN date TYPE TEXT USING date::TEXT,
            ALTER COLUMN rate TYPE TEXT USING rate::TEXT;
    END IF;

    ALTER TABLE bronze.currencies
        ALTER COLUMN iso_code DROP NOT NULL,
        ALTER COLUMN iso_numeric DROP NOT NULL,
        ALTER COLUMN name DROP NOT NULL,
        ALTER COLUMN symbol DROP NOT NULL,
        ALTER COLUMN start_date DROP NOT NULL,
        ALTER COLUMN end_date DROP NOT NULL;

    ALTER TABLE bronze.rates
        ALTER COLUMN date DROP NOT NULL,
        ALTER COLUMN base DROP NOT NULL,
        ALTER COLUMN quote DROP NOT NULL,
        ALTER COLUMN rate DROP NOT NULL;
END
$$;

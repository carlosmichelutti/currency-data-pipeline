CREATE TABLE IF NOT EXISTS gold.fact_exchange_rate (
    date_key INTEGER NOT NULL,
    base_currency_key INTEGER NOT NULL,
    quote_currency_key INTEGER NOT NULL,
    rate DOUBLE PRECISION NOT NULL
        CHECK (rate > 0 AND rate < 'Infinity'::DOUBLE PRECISION),
    PRIMARY KEY (date_key, base_currency_key, quote_currency_key),
    CONSTRAINT fk_exchange_rate_date FOREIGN KEY (date_key)
        REFERENCES gold.dim_date (date_key),
    CONSTRAINT fk_exchange_rate_base FOREIGN KEY (base_currency_key)
        REFERENCES gold.dim_currency (currency_key),
    CONSTRAINT fk_exchange_rate_quote FOREIGN KEY (quote_currency_key)
        REFERENCES gold.dim_currency (currency_key)
);

CREATE INDEX IF NOT EXISTS ix_exchange_rate_base
    ON gold.fact_exchange_rate (base_currency_key, date_key);
CREATE INDEX IF NOT EXISTS ix_exchange_rate_quote
    ON gold.fact_exchange_rate (quote_currency_key, date_key);

COMMENT ON TABLE gold.fact_exchange_rate IS
    'Daily grain: date, base currency, quote currency. Latest successfully executed load wins for overlapping grains.';
COMMENT ON COLUMN gold.fact_exchange_rate.rate IS
    'One unit of base equals rate units of quote. Non-additive Float64 measure, preserved from silver without rounding.';

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE CHECK (date BETWEEN DATE '0001-01-01' AND DATE '9999-12-31'),
    year INTEGER NOT NULL CHECK (year = EXTRACT(YEAR FROM date)::INTEGER),
    quarter SMALLINT NOT NULL CHECK (quarter = EXTRACT(QUARTER FROM date)::SMALLINT),
    month SMALLINT NOT NULL CHECK (month = EXTRACT(MONTH FROM date)::SMALLINT),
    day SMALLINT NOT NULL CHECK (day = EXTRACT(DAY FROM date)::SMALLINT),
    iso_year INTEGER NOT NULL CHECK (iso_year = EXTRACT(ISOYEAR FROM date)::INTEGER),
    iso_week SMALLINT NOT NULL CHECK (iso_week = EXTRACT(WEEK FROM date)::SMALLINT),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week = EXTRACT(ISODOW FROM date)::SMALLINT),
    is_weekend BOOLEAN NOT NULL CHECK (is_weekend = (EXTRACT(ISODOW FROM date) >= 6)),
    CHECK (date_key = year * 10000 + month * 100 + day)
);

COMMENT ON TABLE gold.dim_date IS
    'Continuous calendar within each loaded batch date range. ISO weekday: Monday=1, Sunday=7.';
COMMENT ON COLUMN gold.dim_date.date_key IS 'Stable YYYYMMDD integer key.';

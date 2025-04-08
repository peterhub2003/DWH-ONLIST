DROP TABLE IF EXISTS dwh.dim_date CASCADE;

CREATE TABLE dwh.dim_date (
    date_key INTEGER PRIMARY KEY, -- Format YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7), -- ISO standard: 1=Monday, 7=Sunday
    day_name VARCHAR(10) NOT NULL,
    day_of_month SMALLINT NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_year SMALLINT NOT NULL CHECK (day_of_year BETWEEN 1 AND 366),
    week_of_year SMALLINT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    month_name VARCHAR(10) NOT NULL,
    month_number SMALLINT NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    quarter SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_weekday BOOLEAN NOT NULL
);

-- Create necessary indexes
CREATE INDEX idx_dim_date_full_date ON dwh.dim_date(full_date);
CREATE INDEX idx_dim_date_year_month_day ON dwh.dim_date(year, month_number, day_of_month);
CREATE INDEX idx_dim_date_year_quarter ON dwh.dim_date(year, quarter);
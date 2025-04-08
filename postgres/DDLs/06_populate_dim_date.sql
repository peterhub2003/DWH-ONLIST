TRUNCATE TABLE dwh.dim_date CASCADE;

-- Insert date records (Adjust date range '2016-01-01' to '2019-12-31' if needed)
INSERT INTO dwh.dim_date (
    date_key,
    full_date,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_name,
    month_number,
    quarter,
    year,
    is_weekend,
    is_weekday
)
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INTEGER AS date_key,
    datum AS full_date,
    EXTRACT(ISODOW FROM datum) AS day_of_week, -- ISO standard: 1=Mon, 7=Sun
    TRIM(TO_CHAR(datum, 'Day')) AS day_name, -- TRIMming removes potential trailing spaces
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(DOY FROM datum) AS day_of_year,
    EXTRACT(WEEK FROM datum) AS week_of_year,
    TRIM(TO_CHAR(datum, 'Month')) AS month_name, -- TRIMming
    EXTRACT(MONTH FROM datum) AS month_number,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(YEAR FROM datum) AS year,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(ISODOW FROM datum) NOT IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekday
FROM (
    -- Generate a series of dates encompassing the data range
    SELECT generate_series('2016-01-01'::DATE, '2019-12-31'::DATE, '1 day'::INTERVAL)::DATE AS datum
) DQ
ORDER BY datum;
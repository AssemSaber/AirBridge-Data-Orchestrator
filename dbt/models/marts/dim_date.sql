WITH dates AS (
    SELECT
        DATEADD(
            day,
            SEQ4(),
            DATE '2023-01-01'
        ) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 1095))  -- ~10 years of dates
)

SELECT
    date_day,
    TO_CHAR(date_day, 'YYYYMMDD') AS date_id,
    YEAR(date_day) AS year,
    MONTH(date_day) AS month,
    DAY(date_day) AS day,
    'Q' || QUARTER(date_day) AS quarter,
    TO_CHAR(date_day, 'YYYY-MM-DD') AS date_string,
    DAYOFWEEK(date_day) AS day_of_week,
    DAYNAME(date_day) AS day_name,
    MONTHNAME(date_day) AS month_name,
    WEEKOFYEAR(date_day) AS week_of_year
FROM dates
WHERE date_day <= CURRENT_DATE()  -- Filter to only dates up to today
ORDER BY date_day
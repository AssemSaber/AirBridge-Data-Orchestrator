WITH times AS (
    SELECT
        SEQ4() AS time_id,
        SEQ4() AS minute_of_day
    FROM TABLE(GENERATOR(ROWCOUNT => 1440))  -- 24 hours * 60 minutes = 1440
),

time_calculations AS (
    SELECT
        time_id + 1 AS time_id,  -- Start from 1 instead of 0
        FLOOR(minute_of_day / 60) AS hour,
        MOD(minute_of_day, 60) AS minute
    FROM times
)

SELECT
    time_id,
    (hour * 100) + minute AS hhmm,
    hour,
    minute,
    CASE
        WHEN hour = 0 THEN 'Midnight'
        WHEN hour < 12 THEN 'Morning'
        WHEN hour = 12 THEN 'Noon'
        WHEN hour < 18 THEN 'Afternoon'
        WHEN hour < 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_period
FROM time_calculations
ORDER BY time_id
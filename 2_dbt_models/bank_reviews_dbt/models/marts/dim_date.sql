{{ config(
    materialized='table',
    indexes=[
      {'columns': ['date_key'], 'type': 'btree', 'unique': True},
      {'columns': ['full_date'], 'type': 'btree'},
      {'columns': ['year', 'month'], 'type': 'btree'}
    ]
) }}

/*
DIM_DATE - Date Dimension Table
Comprehensive date dimension for time intelligence
Covers date range from 2020 to 2030 for historical and future analysis
*/

WITH date_spine AS (
    SELECT 
        generate_series(
            '2020-01-01'::date,
            '2030-12-31'::date,
            '1 day'::interval
        )::date AS full_date
),

date_calculations AS (
    SELECT 
        full_date,
        
        -- Date key in YYYYMMDD format
        EXTRACT(YEAR FROM full_date) * 10000 + 
        EXTRACT(MONTH FROM full_date) * 100 + 
        EXTRACT(DAY FROM full_date) AS date_key,
        
        -- Year attributes
        EXTRACT(YEAR FROM full_date) AS year,
        EXTRACT(QUARTER FROM full_date) AS quarter,
        
        -- Month attributes
        EXTRACT(MONTH FROM full_date) AS month,
        TO_CHAR(full_date, 'Month') AS month_name,
        
        -- Week attributes  
        EXTRACT(WEEK FROM full_date) AS week_of_year,
        
        -- Day attributes
        EXTRACT(DAY FROM full_date) AS day_of_month,
        EXTRACT(DOW FROM full_date) AS day_of_week, -- 0=Sunday, 6=Saturday
        TO_CHAR(full_date, 'Day') AS day_name,
        
        -- Weekend flag
        CASE 
            WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        -- Season (Northern Hemisphere)
        CASE 
            WHEN EXTRACT(MONTH FROM full_date) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM full_date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM full_date) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM full_date) IN (9, 10, 11) THEN 'Autumn'
        END AS season,
        
        -- Fiscal year (assuming January-December)
        EXTRACT(YEAR FROM full_date) AS fiscal_year,
        EXTRACT(QUARTER FROM full_date) AS fiscal_quarter,
        
        -- Holiday flags for Morocco (approximate major holidays)
        CASE 
            WHEN (EXTRACT(MONTH FROM full_date) = 1 AND EXTRACT(DAY FROM full_date) = 1) -- New Year
                OR (EXTRACT(MONTH FROM full_date) = 1 AND EXTRACT(DAY FROM full_date) = 11) -- Independence Manifesto Day
                OR (EXTRACT(MONTH FROM full_date) = 5 AND EXTRACT(DAY FROM full_date) = 1) -- Labour Day
                OR (EXTRACT(MONTH FROM full_date) = 7 AND EXTRACT(DAY FROM full_date) = 30) -- Throne Day
                OR (EXTRACT(MONTH FROM full_date) = 8 AND EXTRACT(DAY FROM full_date) = 14) -- Oued Ed-Dahab Day
                OR (EXTRACT(MONTH FROM full_date) = 8 AND EXTRACT(DAY FROM full_date) = 20) -- Revolution Day
                OR (EXTRACT(MONTH FROM full_date) = 8 AND EXTRACT(DAY FROM full_date) = 21) -- Youth Day
                OR (EXTRACT(MONTH FROM full_date) = 11 AND EXTRACT(DAY FROM full_date) = 6) -- Green March Day
                OR (EXTRACT(MONTH FROM full_date) = 11 AND EXTRACT(DAY FROM full_date) = 18) -- Independence Day
            THEN TRUE
            ELSE FALSE
        END AS is_holiday
        
    FROM date_spine
)

SELECT 
    date_key,
    full_date,
    year,
    quarter,
    month,
    TRIM(month_name) as month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    TRIM(day_name) as day_name,
    is_weekend,
    is_holiday,
    season,
    fiscal_year,
    fiscal_quarter
    
FROM date_calculations
ORDER BY full_date 
{{ config(materialized='view') }}

SELECT
  hour_of_day,
  day_of_week,
  borough,
  transit_mode,
  AVG(ridership) AS avg_hourly_ridership
FROM {{ ref('stg_mta_ridership') }}
GROUP BY hour_of_day, day_of_week, borough, transit_mode
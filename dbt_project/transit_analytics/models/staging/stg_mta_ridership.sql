-- models/staging/stg_mta_ridership.sql
{{ config(materialized='view') }}

SELECT
  transit_timestamp,
  transit_timestamp::DATE AS ride_date,
  EXTRACT(HOUR FROM transit_timestamp) AS hour_of_day,
  EXTRACT(DOW FROM transit_timestamp) AS day_of_week,
  transit_mode,
  station_complex_id,
  station_complex,
  borough,
  payment_method,
  fare_class_category,
  CAST(ridership AS INTEGER) AS ridership,
  CAST(transfers AS INTEGER) AS transfers,
  latitude,
  longitude
FROM {{ source('raw', 'raw_mta_ridership') }}


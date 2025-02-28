{{ config(materialized='view') }}

SELECT
  transit_timestamp,
  DATE(transit_timestamp) AS ride_date,
  transit_mode,
  station_complex_id,
  station_complex,
  borough,
  payment_method,
  fare_class_category,
  CAST(ridership AS INTEGER) AS ridership,
  CAST(transfers AS INTEGER) AS transfers,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude
FROM {{ source('raw', 'raw_mta_ridership') }}
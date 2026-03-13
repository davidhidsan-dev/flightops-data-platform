-- Example queries for the published BigQuery table:
-- `airport_hourly_operations_enriched`

-- 1. Daily traffic by airport
SELECT
  airport_icao,
  DATE(operation_hour_utc) AS operation_date,
  SUM(observed_arrivals_count) AS total_arrivals,
  SUM(observed_departures_count) AS total_departures,
  SUM(total_flights_observed) AS total_flights
FROM `datasets-490115.flightops.airport_hourly_operations_enriched`
GROUP BY
  airport_icao,
  operation_date
ORDER BY
  operation_date,
  airport_icao;


-- 2. Top traffic hours by airport
SELECT
  airport_icao,
  operation_hour_utc,
  total_flights_observed,
  observed_arrivals_count,
  observed_departures_count
FROM `datasets-490115.flightops.airport_hourly_operations_enriched`
ORDER BY
  total_flights_observed DESC,
  airport_icao,
  operation_hour_utc
LIMIT 20;


-- 3. Rainy and high-traffic hours
SELECT
  airport_icao,
  operation_hour_utc,
  total_flights_observed,
  precipitation_mm,
  wind_speed_10m_kmh,
  is_rainy_hour,
  is_high_traffic_hour
FROM `datasets-490115.flightops.airport_hourly_operations_enriched`
WHERE
  is_rainy_hour = TRUE
  AND is_high_traffic_hour = TRUE
ORDER BY
  operation_hour_utc;
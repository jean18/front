WITH lagged_data AS (
  SELECT
    stations.station_id,
    station_name,
    wind_speed,
    LAG(wind_speed) OVER (ORDER BY observation_timestamp) AS previous_wind_speed
  FROM
    weather_obs
  INNER JOIN
    stations
  ON weather_obs.station_id = stations.station_id
  WHERE
    observation_timestamp BETWEEN (CURRENT_DATE() - INTERVAL 7 DAY) AND CURRENT_DATE()
)
SELECT
  station_id,
  station_name,
  ROUND(MAX(ABS((wind_speed - previous_wind_speed))), 2) AS max_wind_speed_change
FROM
  lagged_data
GROUP BY 1,2;

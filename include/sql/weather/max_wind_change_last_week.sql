WITH lagged_data AS (
  SELECT
    station_id,
    wind_speed,
    LAG(wind_speed) OVER (ORDER BY observation_timestamp) AS previous_wind_speed
  FROM
    weather_obs
  WHERE
    observation_timestamp BETWEEN (CURRENT_DATE() - INTERVAL 7 DAY) AND CURRENT_DATE()
)
SELECT
  stations.station_id,
  station_name,
  ROUND(MAX(ABS((wind_speed - previous_wind_speed))), 2) AS max_wind_speed_change
FROM
  lagged_data
INNER JOIN
  stations
ON lagged_data.station_id = stations.station_id
GROUP BY 1,2;

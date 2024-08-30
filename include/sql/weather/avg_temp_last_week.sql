SELECT
    stations.station_id,
    station_name,
    AVG(temperature) AS average_temperature
FROM
    weather_obs
INNER JOIN
    stations
ON weather_obs.station_id = stations.station_id
WHERE
    observation_timestamp BETWEEN (current_date() - INTERVAL 7 DAY) AND current_date()
GROUP BY
    1,2;

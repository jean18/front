INSERT INTO weather_obs
SELECT
    station_id,
    latitude,
    longitude,
    observation_timestamp,
    ROUND(temperature, 2) AS temperature,
    ROUND(wind_speed, 2) AS wind_speed,
    ROUND(humidity, 2) AS humidity
FROM
    READ_PARQUET("{{ task_instance.xcom_pull(task_ids='weather_obs.extract_data', key='return_value') }}");

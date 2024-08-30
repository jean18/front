CREATE OR REPLACE TABLE weather_obs (
    station_id VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    observation_timestamp TIMESTAMP,
    temperature DOUBLE,
    wind_speed DOUBLE,
    humidity DOUBLE
);

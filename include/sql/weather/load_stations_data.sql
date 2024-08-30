TRUNCATE TABLE stations;

INSERT INTO stations
SELECT
    station_id,
    station_name,
    station_timezone
FROM
    READ_PARQUET("{{ task_instance.xcom_pull(task_ids='stations.extract_data', key='return_value') }}");

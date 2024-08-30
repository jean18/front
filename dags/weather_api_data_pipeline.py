"""
DAG to load data between Weather API and Duck DB.
"""
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import include.scripts.commons.dag_utils as dag_utils
from include.scripts.weather.utils import (
    STATIONS,
    WEATHER_OBS,
    extract_stations_data,
    extract_weather_obs_data,
    get_start_param,
    load_extracted_data,
)

DAG_NAME: str = "weather_api_data_pipeline"
DEFAULT_ARGS: Dict[str, Any] = dag_utils.get_default_args(
    start_date=datetime(2024, 8, 25)
)

with DAG(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath=dag_utils.get_template_searchpath(),
) as dag:
    start: EmptyOperator = EmptyOperator(task_id="start")

    with TaskGroup(group_id=f"{STATIONS.name}") as station_task_group:
        extract_data: PythonOperator = PythonOperator(
            task_id="extract_data",
            python_callable=extract_stations_data,
            op_kwargs={"ts": "{{ ts }}"},
        )

        load_data: PythonOperator = PythonOperator(
            task_id="load_data",
            python_callable=load_extracted_data,
            op_kwargs={"sql_query": f"{{% include  '{STATIONS.sql_path}' %}}"},
        )

        extract_data >> load_data

    with TaskGroup(group_id=f"{WEATHER_OBS.name}") as weather_obs_task_group:
        start_param: PythonOperator = PythonOperator(
            task_id="start_param",
            python_callable=get_start_param,
            op_kwargs={
                "start_date": "{{ data_interval_start }}",
                "last_end_date": "{{ var.value.weather_obs_last_date }}",
            },
        )

        extract_data: PythonOperator = PythonOperator(
            task_id="extract_data",
            python_callable=extract_weather_obs_data,
            op_kwargs={
                "ts": "{{ ts }}",
                "start": "{{ task_instance.xcom_pull(task_ids='weather_obs.start_param', key='return_value') }}",
            },
        )

        load_data: PythonOperator = PythonOperator(
            task_id="load_data",
            python_callable=load_extracted_data,
            op_kwargs={"sql_query": f"{{% include  '{WEATHER_OBS.sql_path}' %}}"},
        )

        start_param >> extract_data >> load_data

    end = EmptyOperator(task_id="end")

    start >> station_task_group >> weather_obs_task_group >> end

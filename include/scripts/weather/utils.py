"""Util script to extract and load the data from weather API."""
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, NamedTuple, Optional, Union

import duckdb
import pandas as pd
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from include.scripts.weather.client import WeatherClient, WeatherEndpoints

NULL_VALUE = None
SELECTED_STATION_ID: str = "0112W"
DUCK_DB: str = "include/database/duck.db"


class TableMetadata(NamedTuple):
    """Table Metadata."""

    name: str
    sql_path: str


STATIONS: TableMetadata = TableMetadata(
    name="stations", sql_path="sql/weather/load_stations_data.sql"
)
WEATHER_OBS: TableMetadata = TableMetadata(
    name="weather_obs", sql_path="sql/weather/load_weather_obs_data.sql"
)


def get_start_param(start_date: str, last_end_date: str) -> Optional[str]:
    """Get the start date param for the weather obs.

    Args:
        `start_date`: The DAG run start date.
        `last_end_date`: The last end date ingested in the weather obs table.

    Returns:
        The start date param required for the weather obs endpoint.
        If the run was already proccesed it will trigger an Airflow Skip
        Exception.
    """
    start: str
    if last_end_date == NULL_VALUE:
        start = (datetime.fromisoformat(start_date) - timedelta(days=7)).isoformat()
        logging.info("Will process last 7-days.")
    else:
        if datetime.fromisoformat(start_date) < datetime.fromisoformat(last_end_date):
            logging.info("The data of this run was already loaded.")
            raise AirflowSkipException("Skipping downstream tasks.")
        else:
            # Apply 1 second delay because start is an inclusive date in the API.
            # Assumption: The frequency of data can't be less or equal to 1 second
            start = (
                datetime.fromisoformat(last_end_date) + timedelta(seconds=1)
            ).isoformat()
            logging.info(f"Will use start date: {start} to retrieve data.")
    return start


def extract_weather_obs_data(ts: str, start: str) -> str:
    """Extract the weather obs data from Weather API.

    Args:
        `ts`: The DAG run start date.
        `start`: The param to specify from when extract
            data from the weather obs endpoint.

    Returns:
        Path where the raw data obtained from the API request
        was stored.
    """
    weather_client: WeatherClient = WeatherClient()
    station_obs_endpoint: str = os.path.join(
        WeatherEndpoints.STATIONS.value,
        SELECTED_STATION_ID,
        WeatherEndpoints.OBSERVATIONS.value,
    )
    params: Dict[str, str] = {
        "start": start,
    }
    data: Optional[Dict[str, Any]] = weather_client.make_request(
        endpoint=station_obs_endpoint, params=params
    )

    extracted_data: List[Dict[str, str]] = [
        extract_weather_fields(feature) for feature in data["features"]
    ]

    if len(extracted_data) == 0:
        logging.info("No new data to ingest.")
        raise AirflowSkipException("Skipping downstream tasks.")

    extracted_data_sorted = sorted(
        extracted_data, key=lambda x: x["observation_timestamp"]
    )

    last_observation_timestamp: str = extracted_data_sorted[-1]["observation_timestamp"]
    logging.info(f"Number of rows retrieved: {len(extracted_data_sorted)}")
    logging.info(
        "Updating the var weather_obs_last_date with value: "
        f"{last_observation_timestamp}"
    )
    Variable.set("weather_obs_last_date", last_observation_timestamp)

    saved_file_path: str = save_data_to_disk(
        data=extracted_data_sorted, table_name=WEATHER_OBS.name, ts=ts
    )

    return saved_file_path


def extract_stations_data(ts: str) -> str:
    """Extract the stations data from Weather API.

    Args:
        `ts`: The DAG run start date.

    Returns:
        Path where the raw data obtained from the API request
        was stored.
    """
    weather_client: WeatherClient = WeatherClient()

    station_obs_endpoint: str = os.path.join(
        WeatherEndpoints.STATIONS.value,
        SELECTED_STATION_ID,
    )

    data: Optional[Dict[str, Any]] = weather_client.make_request(
        endpoint=station_obs_endpoint
    )
    extracted_data: List[Dict[str, str]] = [extract_stations_fields(data["properties"])]

    saved_file_path: str = save_data_to_disk(
        data=extracted_data, table_name=STATIONS.name, ts=ts
    )

    return saved_file_path


def load_extracted_data(sql_query: str) -> None:
    """Load extracted data using a sql query.

    Args:
        `sql_query`: SQL query that contains the logic
            to inges the raw data extracted from the API
            into the specified table.

    Returns:
        None, only execute the query.
    """
    with duckdb.connect(DUCK_DB) as con:
        logging.info(f"Executing query: \n {sql_query}")
        con.execute(sql_query)
        logging.info("Done :)")


def extract_weather_fields(feature: Dict[str, Any]) -> Dict[str, Union[str, float]]:
    """Extract the required data for weather_obs table.

    Args:
        `feature`: This is a dictionary that contains a feature
            of an observation.

    Returns:
        The required data needed to ingest into the weather obs table.
    """
    station_id: str = SELECTED_STATION_ID
    latitude: float
    longitude: float
    latitude, longitude = feature.get("geometry", {}).get("coordinates", NULL_VALUE)
    feature_properties: Dict[str, Any] = feature.get("properties", {})
    observation_timestamp: str = feature_properties.get("timestamp", NULL_VALUE)
    temperature: float = feature_properties.get("temperature", {}).get(
        "value", NULL_VALUE
    )
    wind_speed: float = feature_properties.get("windSpeed", {}).get("value", NULL_VALUE)
    humidity: float = feature_properties.get("relativeHumidity", {}).get(
        "value", NULL_VALUE
    )
    return {
        "station_id": station_id,
        "latitude": latitude,
        "longitude": longitude,
        "observation_timestamp": observation_timestamp,
        "temperature": temperature,
        "wind_speed": wind_speed,
        "humidity": humidity,
    }


def extract_stations_fields(properties: Dict[str, Any]) -> Dict[str, str]:
    """Extract the required data for stations table.

    Args:
        `properties`: This is a dictionary that contains
            metadata of a station.

    Returns:
        The required data needed to ingest into the stations table.
    """
    station_id: str = SELECTED_STATION_ID
    station_name: str = properties.get("name", NULL_VALUE)
    station_timezone: str = properties.get("timeZone", NULL_VALUE)
    return {
        "station_id": station_id,
        "station_name": station_name,
        "station_timezone": station_timezone,
    }


def save_data_to_disk(data: List[Dict[str, Any]], table_name: str, ts: str) -> str:
    """Save raw data as a parquet file using snappy compression.

    Args:
        `data`: List of dictionaries that contains the data to save.
        `table_name`: Name of the table that will receive this data.
        `ts`: The DAG run start date.

    Returns:
        The path where the raw data was stored.
    """
    raw_folder: str = os.path.join(os.getcwd(), "raw/weather_api")
    os.makedirs(raw_folder, exist_ok=True)

    raw_file_path: str = f"{raw_folder}/{table_name}_{ts}.parquet"

    df: pd.DataFrame = pd.DataFrame(data)
    with open(raw_file_path, "wb") as file:
        df.to_parquet(file, compression="snappy")
        logging.info(f"Saved data into: {raw_file_path}")
    return raw_file_path

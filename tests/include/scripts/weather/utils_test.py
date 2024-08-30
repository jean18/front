"""Script to test utils for weather API pipeline."""

from typing import Any, Dict, List, Union
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from airflow.exceptions import AirflowSkipException

import include.scripts.weather.utils as utils
from include.scripts.weather.utils import DUCK_DB, NULL_VALUE, SELECTED_STATION_ID


class TestUtils(TestCase):
    """Test utils for weather API pipeline."""

    def setUp(self) -> None:
        """Set up test properties."""
        self.ts: str = "2024-08-29T03:11:13.230100+00:00"
        self.start_date: str = "2024-08-29 03:11:13.230100+00:00"

    def test_get_start_param(self) -> None:
        """Test for get_start_param function."""
        # First case when the last processed date is null (empty)
        # We will process the last 7 days of data.
        last_end_date: str = NULL_VALUE
        response: str = utils.get_start_param(
            start_date=self.start_date, last_end_date=last_end_date
        )
        expected_response: str = "2024-08-22T03:11:13.230100+00:00"
        assert response == expected_response

        # Second case when the start_date < last_end_date
        # and last_end_date != NULL_VALUE
        # Should trigger and AirflowSkipException.
        start_date: str = "2024-08-28T02:40:00+00:00"
        last_end_date: str = "2024-08-29T02:40:00+00:00"
        try:
            utils.get_start_param(start_date=start_date, last_end_date=last_end_date)
        except AirflowSkipException as error:
            assert str(error) == "Skipping downstream tasks."
        else:
            raise AssertionError("Function did not raise an AirflowSkipException")

        # Third case when start_date > last_end_date
        # and last_end_date != NULL_VALUE
        # Should ingest only new data using last_end_date + delta (1 sec).
        start_date: str = "2024-08-30T02:40:00+00:00"
        last_end_date: str = "2024-08-29T02:40:00+00:00"
        response: str = utils.get_start_param(
            start_date=self.start_date, last_end_date=last_end_date
        )
        expected_response: str = "2024-08-29T02:40:01+00:00"
        assert response == expected_response

    def test_cast_none_values(self) -> None:
        """Test for cast_none_values function."""
        value_none = None
        response_none: str = utils.cast_none_values(value=value_none)
        expected_response_none: str = NULL_VALUE

        assert response_none == expected_response_none

        value_float: float = 1.36
        response_float: float = utils.cast_none_values(value=value_float)
        expected_response_float: float = 1.36

        assert response_float == expected_response_float

    def test_extract_weather_fields(self) -> None:
        """Test for extract_weather_fields function."""
        feature: Dict[str, Any] = {
            "geometry": {"coordinates": [-83.17, 30.05], "type": "Point"},
            "id": "https://api.weather.gov/stations/0112W/observations/2024-08-30T09:20:00+00:00",
            "properties": {
                "@id": "https://api.weather.gov/stations/0112W/observations/2024-08-30T09:20:00+00:00",
                "@type": "wx:ObservationStation",
                "barometricPressure": {
                    "qualityControl": "V",
                    "unitCode": "wmoUnit:Pa",
                    "value": 101794.8,
                },
                "cloudLayers": [],
                "dewpoint": {
                    "qualityControl": "V",
                    "unitCode": "wmoUnit:degC",
                    "value": 21.89,
                },
                "elevation": {"unitCode": "wmoUnit:m", "value": 28},
                "heatIndex": {
                    "qualityControl": "V",
                    "unitCode": "wmoUnit:degC",
                    "value": 23.217337393870555,
                },
                "icon": None,
                "maxTemperatureLast24Hours": {
                    "unitCode": "wmoUnit:degC",
                    "value": None,
                },
                "minTemperatureLast24Hours": {
                    "unitCode": "wmoUnit:degC",
                    "value": None,
                },
                "precipitationLast3Hours": {
                    "qualityControl": "Z",
                    "unitCode": "wmoUnit:mm",
                    "value": None,
                },
                "presentWeather": [],
                "rawMessage": "",
                "relativeHumidity": {
                    "qualityControl": "V",
                    "unitCode": "wmoUnit:percent",
                    "value": None,
                },
                "seaLevelPressure": {
                    "qualityControl": "Z",
                    "unitCode": "wmoUnit:Pa",
                    "value": None,
                },
                "station": "https://api.weather.gov/stations/0112W",
                "temperature": {
                    "qualityControl": "V",
                    "unitCode": "wmoUnit:degC",
                    "value": 22.39,
                },
                "textDescription": "",
                "timestamp": "2024-08-30T09:20:00+00:00",
                "visibility": {
                    "qualityControl": "Z",
                    "unitCode": "wmoUnit:m",
                    "value": None,
                },
                "windChill": {
                    "qualityControl": "V",
                    "unitCode": "wmoUnit:degC",
                    "value": None,
                },
                "windDirection": {
                    "qualityControl": "Z",
                    "unitCode": "wmoUnit:degree_(angle)",
                    "value": None,
                },
                "windGust": {
                    "qualityControl": "Z",
                    "unitCode": "wmoUnit:km_h-1",
                    "value": None,
                },
                "windSpeed": {
                    "qualityControl": "V",
                    "unitCode": "wmoUnit:km_h-1",
                    "value": 0,
                },
            },
            "type": "Feature",
        }
        response: Dict[str, Union[str, float]] = utils.extract_weather_fields(
            feature=feature
        )
        expected_response: Dict[str, Union[str, float]] = {
            "station_id": SELECTED_STATION_ID,
            "latitude": -83.17,
            "longitude": 30.05,
            "observation_timestamp": "2024-08-30T09:20:00+00:00",
            "temperature": 22.39,
            "wind_speed": 0,
            "humidity": NULL_VALUE,
        }

        assert response.keys() == expected_response.keys()
        assert all(response[key] == expected_response[key] for key in response)

    def test_extract_stations_fields(self) -> None:
        """Test for extract_stations_fields function."""
        properties: Dict[str, Any] = {
            "@id": "https://api.weather.gov/stations/0112W",
            "@type": "wx:ObservationStation",
            "county": "https://api.weather.gov/zones/county/FLC067",
            "elevation": {"unitCode": "wmoUnit:m", "value": 28.0416},
            "fireWeatherZone": "https://api.weather.gov/zones/fire/FLZ029",
            "forecast": "https://api.weather.gov/zones/forecast/FLZ029",
            "name": "Lafayette High School",
            "stationIdentifier": "0112W",
            "timeZone": "America/New_York",
        }
        response: Dict[str, str] = utils.extract_stations_fields(properties=properties)
        expected_response: Dict[str, str] = {
            "station_id": SELECTED_STATION_ID,
            "station_name": "Lafayette High School",
            "station_timezone": "America/New_York",
        }

        assert response.keys() == expected_response.keys()
        assert all(response[key] == expected_response[key] for key in response)

    @patch("include.scripts.weather.utils.duckdb")
    def test_load_extracted_data(self, duckdb_mock: MagicMock) -> None:
        """Test for load_extracted_data function."""
        sql_query: str = "SELECT 1 FROM table_mock;"

        utils.load_extracted_data(sql_query=sql_query)

        duckdb_mock.assert_has_calls([call.connect(DUCK_DB)])
        duckdb_mock.assert_has_calls([call.connect().__enter__().execute(sql_query)])

    @patch("include.scripts.weather.utils.os")
    @patch("include.scripts.weather.utils.pd")
    @patch("include.scripts.weather.utils.open")
    def test_save_data_to_disk(
        self, open_mock: MagicMock, pd_mock: MagicMock, os_mock: MagicMock
    ) -> None:
        """Test for save_data_to_disk function."""
        current_dir: str = "current_dir"
        base_path: str = f"{current_dir}/raw/weather_api"
        os_mock.getcwd.return_value = current_dir
        os_mock.path.join.return_value = base_path

        data: List[Dict[str, Any]] = [{"field_1": "value_1"}, {"field_2": "value_2"}]
        table_name: str = "table_mock"

        raw_file_path: str = f"{base_path}/{table_name}_{self.ts}.parquet"

        response: str = utils.save_data_to_disk(
            data=data, table_name=table_name, ts=self.ts
        )
        expected_response: str = raw_file_path

        assert response == expected_response

        pd_mock.assert_has_calls([call.DataFrame(data)])
        open_mock.assert_has_calls([call(raw_file_path, "wb")])

    @patch("include.scripts.weather.utils.WeatherClient")
    @patch("include.scripts.weather.utils.extract_stations_fields")
    @patch("include.scripts.weather.utils.save_data_to_disk")
    def test_extract_stations_data(
        self,
        save_data_to_disk_mock: MagicMock,
        extract_stations_fields_mock: MagicMock,
        weather_client_mock: MagicMock,
    ) -> None:
        """Test for extract_stations_data function."""

        response: str = utils.extract_stations_data(ts=self.ts)
        expected_response: str = "None"

        assert response == expected_response

    # @patch("include.scripts.weather.utils.Variable")
    # @patch("include.scripts.weather.utils.WeatherClient")
    # @patch("include.scripts.weather.utils.extract_stations_fields")
    # @patch("include.scripts.weather.utils.save_data_to_disk")
    # def test_extract_weather_obs_data(
    #     self,
    #     save_data_to_disk_mock: MagicMock,
    #     extract_stations_fields_mock: MagicMock,
    #     weather_client_mock: MagicMock,
    #     variable_mock: MagicMock
    #     ) -> None:
    #     """Test for extract_weather_obs_data function."""

    #     response: str = utils.extract_weather_obs_data(
    #         ts=self.ts,
    #         start=self.start_date
    #     )
    #     expected_response: str = "None"

    #     assert response == expected_response

    #     extracted_data: List[Dict[str, str]] = [extract_stations_fields(data["properties"])]

    # saved_file_path: str = save_data_to_disk(

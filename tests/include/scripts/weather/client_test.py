"""Script to test WeatherClient class."""

from typing import Any, Dict
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from include.scripts.weather.client import WeatherClient


class TestWeatherClient(TestCase):
    """Test WeatherClient class."""

    def setUp(self) -> None:
        """Set up test properties."""
        self.client: WeatherClient = WeatherClient()
        self.endpoint: str = "endpoint_mock"
        self.url: str = "https://api.weather.gov/endpoint_mock"
        self.params: Dict[str, Any] = {"param_1": "value_1", "param_2": "value_2"}
        self.headers: Dict[str, str] = {"accept": "mock"}
        self.data = "data_mock"

    @patch("include.scripts.weather.client.requests")
    def test_make_request(self, requests_mock: MagicMock) -> None:
        """Test for make_request function."""
        data_mock: MagicMock = MagicMock()
        data_mock.json.return_value = self.data
        requests_mock.get.return_value = data_mock

        response = self.client.make_request(
            endpoint=self.endpoint, params=self.params, headers=self.headers
        )
        expected_response = self.data

        assert response == expected_response

        requests_mock.assert_has_calls(
            [
                call.get(url=self.url, params=self.params, headers=self.headers),
                call.get().raise_for_status(),
                call.get().json(),
            ]
        )

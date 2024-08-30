"""Util class to interact with Weather API."""
import logging
from enum import Enum
from typing import Any, Dict, Optional

import requests
from requests import Response


class WeatherEndpoints(Enum):
    """Weather Endpoints."""

    STATIONS: str = "stations"
    OBSERVATIONS: str = "observations"


class WeatherClient:
    """Class to interact with the Weather Client API.

    For more details please check the following link:
    * https://www.weather.gov/documentation/services-web-api#/default/obs_stations
    """

    __BASE_URL: str = "https://api.weather.gov"

    def make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Dict[str, str] = {"accept": "application/geo+json"},
    ) -> Optional[Dict[str, Any]]:
        """Make a GET request to the Weather API.

        Args:
            `endpoint`: Endpoint for the API.
            `params`: Params for the API.
            `headers`: Headers for the API. By default it is:
                `application/geo+json` which was obtained from
                the website.

        Returns:
            The data obtained from the request.
        """
        url: str = f"{self.__BASE_URL}/{endpoint}"

        logging.info(
            f"API get call to {url} using the following: \n"
            + f"- params: {params}\n"
            + f"- headers: {headers}\n"
        )

        response: Response = requests.get(url=url, params=params, headers=headers)
        response.raise_for_status()

        logging.info("Done :)")

        data: Dict[str, Any] = response.json()
        print(data)
        return data

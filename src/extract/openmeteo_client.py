import requests
from typing import Any


BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


def _make_request(params: dict[str, Any]) -> dict[str, Any]:
    """
    Send a GET request to the Open-Meteo Historical Weather API
    and return the JSON response.

    Args:
        params (dict[str, Any]): Query parameters for the API request.

    Returns:
        dict[str, Any]: Parsed JSON response from the API.

    Raises:
        requests.exceptions.HTTPError: If the response status code indicates an error.
    """
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def get_hourly_weather(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    """
    Retrieve hourly historical weather data for a given location and date range.

    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.

    Returns:
        dict[str, Any]: JSON response containing hourly weather data.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
        ],
        "timezone": "GMT",
    }

    return _make_request(params)
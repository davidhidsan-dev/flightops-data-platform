import time
from typing import Any

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


def _make_request(params: dict[str, Any]) -> dict[str, Any]:
    """
    Send a GET request to the Open-Meteo Historical Weather API
    and return the JSON response.
    """
    max_retries = 3
    sleep_seconds = 5

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"Sending Open-Meteo request attempt={attempt}/{max_retries}"
            )

            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()

            logger.info(
                f"Open-Meteo request succeeded attempt={attempt}/{max_retries}"
            )
            return response.json()

        except Exception as error:
            if attempt == max_retries:
                logger.error(
                    f"Open-Meteo request failed after {max_retries} attempts "
                    f"error={error}"
                )
                raise

            logger.warning(
                f"Open-Meteo request failed attempt={attempt}/{max_retries} "
                f"error={error}. Retrying in {sleep_seconds}s"
            )
            time.sleep(sleep_seconds)

    raise RuntimeError("Unexpected retry flow for Open-Meteo request")


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
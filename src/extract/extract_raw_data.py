from pathlib import Path
import json

from src.config import RAW_DIR
from src.extract.opensky_client import (
    get_arrivals_by_airport,
    get_departures_by_airport,
)
from src.extract.openmeteo_client import get_hourly_weather
from src.utils.logger import get_logger

logger = get_logger(__name__)


def extract_arrivals_raw(
    airport_icao: str,
    begin: int,
    end: int,
    run_date: str,
) -> Path:
    """
    Extract raw arrivals data from OpenSky for a given airport and time window,
    save it locally as JSON, and return the output path.
    """
    logger.info(
        f"Requesting OpenSky arrivals airport={airport_icao} begin={begin} end={end}"
    )

    arrivals = get_arrivals_by_airport(
        airport_icao=airport_icao,
        begin=begin,
        end=end,
    )

    logger.info(
        f"Received arrivals records={len(arrivals)} for airport={airport_icao}"
    )

    if len(arrivals) == 0:
        logger.warning(
            f"No arrivals records returned for airport={airport_icao} date={run_date}"
        )

    output_dir = RAW_DIR / "opensky"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"arrivals_{airport_icao}_{run_date}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(arrivals, file, indent=2, ensure_ascii=False)

    logger.info(f"Arrivals raw JSON saved to {output_path}")
    return output_path


def extract_departures_raw(
    airport_icao: str,
    begin: int,
    end: int,
    run_date: str,
) -> Path:
    """
    Extract raw departures data from OpenSky for a given airport and time window,
    save it locally as JSON, and return the output path.
    """
    logger.info(
        f"Requesting OpenSky departures airport={airport_icao} begin={begin} end={end}"
    )

    departures = get_departures_by_airport(
        airport_icao=airport_icao,
        begin=begin,
        end=end,
    )

    logger.info(
        f"Received departures records={len(departures)} for airport={airport_icao}"
    )

    if len(departures) == 0:
        logger.warning(
            f"No departures records returned for airport={airport_icao} date={run_date}"
        )

    output_dir = RAW_DIR / "opensky"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"departures_{airport_icao}_{run_date}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(departures, file, indent=2, ensure_ascii=False)

    logger.info(f"Departures raw JSON saved to {output_path}")
    return output_path


def extract_weather_raw(
    airport_icao: str,
    latitude: float,
    longitude: float,
    run_date: str,
) -> Path:
    """
    Extract raw hourly weather data from Open-Meteo for a given airport location
    and date, save it locally as JSON, and return the output path.
    """
    logger.info(
        f"Requesting Open-Meteo weather airport={airport_icao} "
        f"date={run_date} latitude={latitude} longitude={longitude}"
    )

    weather = get_hourly_weather(
        latitude=latitude,
        longitude=longitude,
        start_date=run_date,
        end_date=run_date,
    )

    hourly_records = len(weather["hourly"]["time"])
    logger.info(
        f"Received weather hourly records={hourly_records} for airport={airport_icao}"
    )

    if hourly_records == 0:
        logger.warning(
            f"No weather hourly records returned for airport={airport_icao} date={run_date}"
        )

    output_dir = RAW_DIR / "openmeteo"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"weather_{airport_icao}_{run_date}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(weather, file, indent=2, ensure_ascii=False)

    logger.info(f"Weather raw JSON saved to {output_path}")
    return output_path
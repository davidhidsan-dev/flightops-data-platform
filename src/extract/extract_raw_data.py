from pathlib import Path
import json

from src.config import RAW_DIR
from src.extract.opensky_client import (
    get_arrivals_by_airport,
    get_departures_by_airport,
)
from src.extract.openmeteo_client import get_hourly_weather


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
    arrivals = get_arrivals_by_airport(
        airport_icao=airport_icao,
        begin=begin,
        end=end,
    )

    output_dir = RAW_DIR / "opensky"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"arrivals_{airport_icao}_{run_date}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(arrivals, file, indent=2, ensure_ascii=False)

    print(f"Arrivals raw JSON saved to: {output_path}")
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
    departures = get_departures_by_airport(
        airport_icao=airport_icao,
        begin=begin,
        end=end,
    )

    output_dir = RAW_DIR / "opensky"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"departures_{airport_icao}_{run_date}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(departures, file, indent=2, ensure_ascii=False)

    print(f"Departures raw JSON saved to: {output_path}")
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
    weather = get_hourly_weather(
        latitude=latitude,
        longitude=longitude,
        start_date=run_date,
        end_date=run_date,
    )

    output_dir = RAW_DIR / "openmeteo"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"weather_{airport_icao}_{run_date}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(weather, file, indent=2, ensure_ascii=False)

    print(f"Weather raw JSON saved to: {output_path}")
    return output_path
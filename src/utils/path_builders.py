from pathlib import Path

from src.config import RAW_DIR, DATA_DIR


def build_arrivals_raw_path(airport_icao: str, run_date: str) -> Path:
    return RAW_DIR / "opensky" / f"arrivals_{airport_icao}_{run_date}.json"


def build_departures_raw_path(airport_icao: str, run_date: str) -> Path:
    return RAW_DIR / "opensky" / f"departures_{airport_icao}_{run_date}.json"


def build_weather_raw_path(airport_icao: str, run_date: str) -> Path:
    return RAW_DIR / "openmeteo" / f"weather_{airport_icao}_{run_date}.json"


def build_arrivals_staging_path(airport_icao: str, run_date: str) -> Path:
    return DATA_DIR / "staging" / f"arrivals_{airport_icao}_{run_date}_table.csv"


def build_departures_staging_path(airport_icao: str, run_date: str) -> Path:
    return DATA_DIR / "staging" / f"departures_{airport_icao}_{run_date}_table.csv"


def build_weather_staging_path(airport_icao: str, run_date: str) -> Path:
    return DATA_DIR / "staging" / f"weather_{airport_icao}_{run_date}_table.csv"


def build_operations_mart_path(airport_icao: str, run_date: str) -> Path:
    return DATA_DIR / "marts" / f"airport_hourly_operations_{airport_icao}_{run_date}.csv"


def build_enriched_mart_path(airport_icao: str, run_date: str) -> Path:
    return DATA_DIR / "marts" / f"airport_hourly_operations_enriched_{airport_icao}_{run_date}.csv"


def build_published_dataset_path() -> Path:
    return DATA_DIR / "published" / "airport_hourly_operations_enriched.csv"
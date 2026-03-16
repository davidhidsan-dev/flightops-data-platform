import argparse
import json
import pandas as pd

from src.config import DATA_DIR
from src.utils.path_builders import (
    build_weather_raw_path,
    build_weather_staging_path,
)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the weather transform script.
    """
    parser = argparse.ArgumentParser(
        description="Transform raw Open-Meteo weather data into a staging CSV file."
    )
    parser.add_argument(
        "--airport-icao",
        required=True,
        help="ICAO code of the airport, for example LEMD.",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date to process in YYYY-MM-DD format.",
    )
    return parser.parse_args()


def transform_weather_dataframe(weather: dict, airport_icao: str) -> pd.DataFrame:
    """
    Transform raw Open-Meteo weather data into a cleaned staging DataFrame.
    """
    hourly_data = weather["hourly"]
    df = pd.DataFrame(hourly_data)

    df["airport_icao"] = airport_icao

    df = df.rename(columns={
        "time": "weather_hour_utc",
        "temperature_2m": "temperature_2m_c",
        "relative_humidity_2m": "relative_humidity_2m_pct",
        "precipitation": "precipitation_mm",
        "wind_speed_10m": "wind_speed_10m_kmh",
    })

    df["weather_hour_utc"] = pd.to_datetime(df["weather_hour_utc"], utc=True)

    df = df[
        [
            "airport_icao",
            "weather_hour_utc",
            "temperature_2m_c",
            "relative_humidity_2m_pct",
            "precipitation_mm",
            "wind_speed_10m_kmh",
        ]
    ]

    return df


def main() -> None:
    """
    Load raw Open-Meteo weather data from a local JSON file and transform it
    into a staging CSV file.
    """
    args = parse_args()
    airport_icao = args.airport_icao
    run_date = args.date

    raw_path = build_weather_raw_path(airport_icao, run_date)

    with raw_path.open("r", encoding="utf-8") as file:
        weather = json.load(file)

    df = transform_weather_dataframe(weather, airport_icao)

    staging_dir = DATA_DIR / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    output_path = build_weather_staging_path(airport_icao, run_date)
    df.to_csv(output_path, index=False)

    print(f"Rows written: {len(df)}")
    print(f"Staging file saved to: {output_path}")


if __name__ == "__main__":
    main()
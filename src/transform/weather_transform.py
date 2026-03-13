import json
import pandas as pd

from src.config import RAW_DIR, DATA_DIR


def main() -> None:
    """
    Load raw Open-Meteo weather data from a local JSON file and transform it
    into a staging DataFrame with standardized column names and UTC time fields.

    The script:
    - reads raw weather data from the raw layer
    - extracts the hourly section of the Open-Meteo response
    - converts it into a tabular structure
    - adds the airport identifier
    - renames weather columns to a cleaner schema
    - converts time fields into UTC datetime values
    - saves the result as a local staging CSV file
    """
    airport_icao = "LEMD"
    weather_date = "2026-03-07"

    raw_path = RAW_DIR / "openmeteo" / f"weather_{airport_icao}_{weather_date}.json"

    with raw_path.open("r", encoding="utf-8") as file:
        weather = json.load(file)

    hourly_data = weather["hourly"]
    df = pd.DataFrame(hourly_data)

    df["airport_icao"] = airport_icao

    df = df.rename(columns={
        "time": "weather_time_utc",
        "temperature_2m": "temperature_2m_c",
        "relative_humidity_2m": "relative_humidity_2m_pct",
        "precipitation": "precipitation_mm",
        "wind_speed_10m": "wind_speed_10m_kmh",
    })

    df["weather_hour_utc"] = df["weather_time_utc"].dt.floor("h")

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

    staging_dir = DATA_DIR / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    output_path = staging_dir / f"weather_{airport_icao}_{weather_date}_table.csv"
    df.to_csv(output_path, index=False)

    print(f"Rows written: {len(df)}")
    print(f"Staging file saved to: {output_path}")
    print("Sample:")
    print(df.head())


if __name__ == "__main__":
    main()
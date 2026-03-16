from pathlib import Path
import json

from src.extract.openmeteo_client import get_hourly_weather


def main() -> None:
    """
    Test the Open-Meteo client for a single airport location and save
    the raw weather response locally.
    """
    airport_icao = "LEMD"
    latitude = 40.471926
    longitude = -3.56264
    start_date = "2026-03-07"
    end_date = "2026-03-07"

    weather = get_hourly_weather(
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
    )

    print(f"Airport: {airport_icao}")
    print(f"Keys returned: {list(weather.keys())}")
    print("Hourly keys:")
    print(weather.get("hourly", {}).keys())

    output_dir = Path("data/raw/openmeteo")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = f"weather_{airport_icao}_{start_date}.json"
    output_path = output_dir / output_filename

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(weather, file, indent=2, ensure_ascii=False)

    print(f"Raw JSON saved to: {output_path}")


if __name__ == "__main__":
    main()
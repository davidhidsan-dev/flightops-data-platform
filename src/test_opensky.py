from pathlib import Path
import json

from src.extract.opensky_client import get_arrivals_by_airport


def main() -> None:
    """
    Test the OpenSky arrivals client for a single airport and save the raw response locally.

    This script:
    - requests arrival data for a fixed airport and time window
    - prints the number of returned records
    - prints a small sample of records
    - stores the raw JSON response under data/raw/opensky/
    """
    airport_icao = "LEMD"
    begin = 1772841600  # 2026-03-07 00:00:00 UTC
    end = 1772928000    # 2026-03-08 00:00:00 UTC

    arrivals = get_arrivals_by_airport(airport_icao, begin, end)

    print(f"Airport: {airport_icao}")
    print(f"Records returned: {len(arrivals)}")
    print("Sample records:")

    for record in arrivals[:2]:
        print(record)

    output_dir = Path("data/raw/opensky")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = f"arrivals_{airport_icao}_2026-03-07.json"
    output_path = output_dir / output_filename

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(arrivals, file, indent=2, ensure_ascii=False)

    print(f"Raw JSON saved to: {output_path}")


if __name__ == "__main__":
    main()
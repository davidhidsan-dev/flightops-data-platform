import json
import pandas as pd

from src.config import RAW_DIR, DATA_DIR


def main() -> None:
    """
    Load raw OpenSky arrivals data from a local JSON file and transform it
    into a staging DataFrame with standardized column names and UTC time fields.

    The script:
    - reads raw arrivals data from the raw layer
    - renames relevant OpenSky fields
    - selects the columns needed for the MVP
    - cleans the callsign field
    - converts Unix timestamps into UTC datetime fields
    - derives an hourly UTC timestamp for later aggregation
    - saves the result as a local staging CSV file
    """
    raw_path = RAW_DIR / "opensky" / "arrivals_LEMD_2026-03-07.json"

    with raw_path.open("r", encoding="utf-8") as file:
        arrivals = json.load(file)

    df = pd.DataFrame(arrivals)

    df = df.rename(columns={
        "firstSeen": "first_seen_unix",
        "lastSeen": "last_seen_unix",
        "estDepartureAirport": "departure_airport_icao",
        "estArrivalAirport": "arrival_airport_icao",
    })

    df = df[
        [
            "icao24",
            "callsign",
            "first_seen_unix",
            "last_seen_unix",
            "departure_airport_icao",
            "arrival_airport_icao",
        ]
    ]

    df["callsign"] = df["callsign"].fillna("").str.strip()

    df["observed_arrival_time_utc"] = pd.to_datetime(
        df["last_seen_unix"],
        unit="s",
        utc=True
    )

    df["observed_arrival_hour_utc"] = df["observed_arrival_time_utc"].dt.floor("h")

    staging_dir = DATA_DIR / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    output_path = staging_dir / "arrivals_LEMD_2026-03-07_table.csv"
    df.to_csv(output_path, index=False)

    print(f"Rows written: {len(df)}")
    print(f"Staging file saved to: {output_path}")


if __name__ == "__main__":
    main()
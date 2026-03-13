import argparse
import json
import pandas as pd

from src.config import RAW_DIR, DATA_DIR


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the departures transform script.
    """
    parser = argparse.ArgumentParser(
        description="Transform raw OpenSky departures data into a staging CSV file."
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


def main() -> None:
    """
    Load raw OpenSky departures data from a local JSON file and transform it
    into a staging DataFrame with standardized column names and UTC time fields.

    The script:
    - reads raw departures data from the raw layer
    - renames relevant OpenSky fields
    - selects the columns needed for the MVP
    - cleans the callsign field
    - converts Unix timestamps into UTC datetime fields
    - derives an hourly UTC timestamp for later aggregation
    - saves the result as a local staging CSV file
    """
    args = parse_args()
    airport_icao = args.airport_icao
    run_date = args.date

    raw_path = RAW_DIR / "opensky" / f"departures_{airport_icao}_{run_date}.json"

    with raw_path.open("r", encoding="utf-8") as file:
        departures = json.load(file)

    df = pd.DataFrame(departures)

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

    df["observed_departure_time_utc"] = pd.to_datetime(
        df["first_seen_unix"],
        unit="s",
        utc=True,
    )

    df["observed_departure_hour_utc"] = df["observed_departure_time_utc"].dt.floor("h")

    staging_dir = DATA_DIR / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    output_path = staging_dir / f"departures_{airport_icao}_{run_date}_table.csv"
    df.to_csv(output_path, index=False)

    print(f"Rows written: {len(df)}")
    print(f"Staging file saved to: {output_path}")


if __name__ == "__main__":
    main()
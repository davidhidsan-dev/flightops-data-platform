import argparse
import json
import pandas as pd

from src.config import DATA_DIR
from src.utils.path_builders import (
    build_arrivals_raw_path,
    build_arrivals_staging_path,
)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the arrivals transform script.
    """
    parser = argparse.ArgumentParser(
        description="Transform raw OpenSky arrivals data into a staging CSV file."
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


def transform_arrivals_dataframe(arrivals: list[dict]) -> pd.DataFrame:
    """
    Transform raw OpenSky arrivals records into a cleaned staging DataFrame.
    """
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
        utc=True,
    )

    df["observed_arrival_hour_utc"] = df["observed_arrival_time_utc"].dt.floor("h")

    return df


def main() -> None:
    """
    Load raw OpenSky arrivals data from a local JSON file and transform it
    into a staging CSV file.
    """
    args = parse_args()
    airport_icao = args.airport_icao
    run_date = args.date

    raw_path = build_arrivals_raw_path(airport_icao, run_date)

    with raw_path.open("r", encoding="utf-8") as file:
        arrivals = json.load(file)

    df = transform_arrivals_dataframe(arrivals)

    staging_dir = DATA_DIR / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    output_path = build_arrivals_staging_path(airport_icao, run_date)
    df.to_csv(output_path, index=False)

    print(f"Rows written: {len(df)}")
    print(f"Staging file saved to: {output_path}")


if __name__ == "__main__":
    main()
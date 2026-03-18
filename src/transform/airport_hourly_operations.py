def build_airport_hourly_operations(
    arrivals_df: pd.DataFrame,
    departures_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the hourly airport operations mart from arrivals and departures staging data.
    """
    arrivals_hourly = (
        arrivals_df.groupby(
            ["arrival_airport_icao", "observed_arrival_hour_utc"],
            as_index=False
        )
        .agg(
            observed_arrivals_count=("icao24", "count"),
            unique_arrival_aircraft_count=("icao24", "nunique"),
        )
        .rename(
            columns={
                "arrival_airport_icao": "airport_icao",
                "observed_arrival_hour_utc": "operation_hour_utc",
            }
        )
    )

    departures_hourly = (
        departures_df.groupby(
            ["departure_airport_icao", "observed_departure_hour_utc"],
            as_index=False
        )
        .agg(
            observed_departures_count=("icao24", "count"),
            unique_departure_aircraft_count=("icao24", "nunique"),
        )
        .rename(
            columns={
                "departure_airport_icao": "airport_icao",
                "observed_departure_hour_utc": "operation_hour_utc",
            }
        )
    )

    operations_df = arrivals_hourly.merge(
        departures_hourly,
        on=["airport_icao", "operation_hour_utc"],
        how="outer",
    )

    count_columns = [
        "observed_arrivals_count",
        "unique_arrival_aircraft_count",
        "observed_departures_count",
        "unique_departure_aircraft_count",
    ]

    operations_df[count_columns] = operations_df[count_columns].fillna(0).astype(int)

    operations_df["total_flights_observed"] = (
        operations_df["observed_arrivals_count"]
        + operations_df["observed_departures_count"]
    )

    # Correct calculation of unique aircraft across arrivals + departures
    arrivals_aircraft = arrivals_df[
        ["arrival_airport_icao", "observed_arrival_hour_utc", "icao24"]
    ].rename(
        columns={
            "arrival_airport_icao": "airport_icao",
            "observed_arrival_hour_utc": "operation_hour_utc",
        }
    )

    departures_aircraft = departures_df[
        ["departure_airport_icao", "observed_departure_hour_utc", "icao24"]
    ].rename(
        columns={
            "departure_airport_icao": "airport_icao",
            "observed_departure_hour_utc": "operation_hour_utc",
        }
    )

    all_aircraft = pd.concat(
        [arrivals_aircraft, departures_aircraft],
        ignore_index=True,
    )

    unique_aircraft_hourly = (
        all_aircraft.groupby(
            ["airport_icao", "operation_hour_utc"],
            as_index=False
        )
        .agg(
            total_unique_aircraft_observed=("icao24", "nunique")
        )
    )

    operations_df = operations_df.merge(
        unique_aircraft_hourly,
        on=["airport_icao", "operation_hour_utc"],
        how="left",
    )

    operations_df["total_unique_aircraft_observed"] = (
        operations_df["total_unique_aircraft_observed"]
        .fillna(0)
        .astype(int)
    )

    operations_df = operations_df.sort_values(
        by=["airport_icao", "operation_hour_utc"]
    ).reset_index(drop=True)

    return operations_df
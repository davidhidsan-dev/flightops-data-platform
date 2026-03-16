# FlightOps Airport Operations Data Pipeline

Versión en español de este README: [README.md](README.md)

## Description

End-to-end data engineering project to extract, transform, enrich, and publish observed airport operations and hourly weather data using Python, OpenSky, Open-Meteo, and BigQuery.

The project builds a batch mini data system that:
- ingests observed airport arrivals and departures
- enriches operational activity with hourly weather data
- publishes a consolidated final dataset ready for analysis
- runs basic data quality validations
- loads the final dataset into BigQuery

## Project goal

Build a reproducible and parameterized pipeline that transforms raw flight and weather data into analytical airport-hour tables.

## What this project demonstrates

- external API data extraction
- raw JSON storage
- staging transformations in Python
- analytical mart construction
- multi-source enrichment
- consolidated dataset publishing
- basic data quality checks
- airport/date parameterization
- end-to-end execution through a pipeline runner
- final loading of the published dataset into BigQuery

## Stack

- Python
- pandas
- SQL
- BigQuery
- OpenSky API
- Open-Meteo API
- Git / GitHub
- VS Code

## Repository structure

- `data/seeds/`: controlled seed data such as the airport dimension
- `src/extract/`: extraction clients and logic for OpenSky and Open-Meteo
- `src/transform/`: transformations from raw to staging, marts, and published dataset
- `src/load/`: final dataset load into BigQuery
- `src/quality/`: basic data quality validations
- `src/run_airport_pipeline.py`: main end-to-end pipeline runner
- `sql/`: space for analytical and consumption SQL queries
- `docs/`: technical project documentation
- `scripts/`: auxiliary scripts and exploratory tests kept outside the main package
- `data/`: local pipeline layers (seed, raw, staging, marts, published)

## Current scope

The current pipeline supports:
- Spanish airports included in the local seed
- batch processing by airport and date
- observed arrivals and departures from OpenSky
- hourly weather data from Open-Meteo
- multi-airport result consolidation
- basic quality checks on the final published dataset
- loading the final consolidated dataset into BigQuery

## Pipeline flow

1. airport seed loading
2. raw arrivals and departures extraction from OpenSky
3. raw weather extraction from Open-Meteo
4. transformation into staging tables
5. construction of `airport_hourly_operations`
6. weather enrichment in `airport_hourly_operations_enriched`
7. publication of a consolidated dataset
8. data quality validation
9. loading the final dataset into BigQuery

## Final dataset

The published output is a consolidated dataset at:
- airport
- UTC hour

It includes:
- observed arrivals
- observed departures
- total observed flights
- hourly weather metrics
- derived flags such as:
  - `is_rainy_hour`
  - `is_high_wind_hour`
  - `is_high_traffic_hour`

## Run instructions

Example full pipeline run:

    python -m src.run_airport_pipeline --airport-icao LEMD --date 2026-03-07

This command runs the full pipeline end to end:
- raw extraction
- staging transformations
- mart construction
- consolidated dataset publishing
- quality checks
- final BigQuery load

The following commands can also be executed independently for development, debugging, or manual reprocessing:

Consolidated publishing:

    python -m src.transform.publish_airport_operations

Quality checks:

    python -m src.quality.check_airport_operations

BigQuery loading:

    python -m src.load.bigquery_loader

## Technical documentation

Additional documentation is available in:

- `docs/architecture.md`
- `docs/source_assumptions.md`

## Current limitations

- the pipeline is batch-based, not real-time
- OpenSky models network-observed activity, not exact official schedules
- current data quality checks are basic
# FlightOps Airport Operations Data Pipeline

Versión en español de este README: [README.md](README.md)

> First real API-based batch pipeline focused on extraction, normalization, enrichment and optional BigQuery loading.

## Description

End-to-end batch data engineering project for extracting observed airport operations from OpenSky and hourly weather from Open-Meteo, transforming the data with Python, and preparing a reproducible analytical layer.

The final output is a consolidated airport-hour dataset with observed activity, weather metrics, and derived flags, ready for local analysis or optional loading into BigQuery.

## Project status

Functional version completed:

- extraction from OpenSky and Open-Meteo
- raw JSON persistence
- staging transformations in Python
- airport-hour dataset construction
- hourly weather enrichment
- basic quality checks
- structured logging and basic retry logic
- optional BigQuery loading

## What this project demonstrates

- extraction from external APIs
- raw JSON persistence
- transformations into staging/marts layers in Python
- multi-source enrichment with weather data
- consolidated airport-hour dataset
- basic quality checks
- structured logging and basic retry logic
- optional BigQuery loading

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

## Pipeline flow

1. airport seed loading
2. extraction from OpenSky and Open-Meteo
3. raw JSON persistence
4. staging/marts transformations in Python
5. published airport-hour dataset
6. basic quality checks
7. optional loading to BigQuery

## Final dataset

The published output is a consolidated airport-hour UTC dataset.

It includes observed arrivals, observed departures, total observed flights, hourly weather metrics, and derived flags such as `is_rainy_hour`, `is_high_wind_hour`, and `is_high_traffic_hour`.

## Run instructions

Example full pipeline run:

```bash
python -m src.run_airport_pipeline --airport-icao LEMD --date 2026-03-07
```

This command runs raw extraction, transformations, dataset publishing, quality checks, and optional BigQuery loading.

During execution:
- the pipeline emits structured logs to the console
- API clients apply basic retries on temporary failures
- if an operational source returns empty results, a warning is generated
- BigQuery loading requires manual confirmation
- if the run contains warnings, an additional confirmation is required before loading

Independent commands for development, debugging, or manual reprocessing:

Consolidated publishing:

```bash
python -m src.transform.publish_airport_operations
```

Quality checks:

```bash
python -m src.quality.check_airport_operations
```

BigQuery loading:

```bash
python -m src.load.bigquery_loader
```

## Technical documentation

Additional documentation is available in:

- [Architecture documentation](docs/architecture.md)
- [Source assumptions](docs/source_assumptions.md)

## Note on development

This project was developed with support from AI tools as programming assistance to accelerate implementation, refactoring, and documentation tasks.

Scope definition, pipeline structure, modeling decisions, assumption validation, and final code review were carried out manually.

## Current limitations

- the pipeline is batch-based, not real-time
- OpenSky models network-observed activity, not exact official schedules
- current data quality checks are basic
- an empty response from an operational source may produce a potentially incomplete run, although the pipeline leaves warnings and requires additional confirmation before loading to BigQuery
# FlightOps Data Platform / Plataforma de Datos de Operaciones Aeroportuarias

## ES — Descripción

Proyecto end-to-end de data engineering para extraer, transformar, enriquecer y publicar datos de operaciones aeroportuarias observadas y meteorología horaria usando Python, OpenSky, Open-Meteo y BigQuery.

El proyecto construye un mini sistema de datos batch que:
- ingiere llegadas y salidas observadas por aeropuerto
- enriquece la actividad operativa con clima horario
- publica un dataset final consolidado listo para análisis
- ejecuta validaciones básicas de calidad de datos
- carga el dataset final en BigQuery

## ES — Objetivo del proyecto

Construir un pipeline reproducible y parametrizable que transforme datos raw de vuelos y clima en tablas analíticas a nivel de aeropuerto y hora.

## ES — Qué demuestra este proyecto

- extracción de datos desde APIs externas
- almacenamiento raw en JSON
- transformaciones staging en Python
- construcción de marts analíticas
- enriquecimiento multi-fuente
- publicación de un dataset consolidado
- data quality checks básicos
- parametrización por aeropuerto y fecha
- ejecución end-to-end mediante un runner
- carga final del dataset publicado a BigQuery

## ES — Stack

- Python
- pandas
- SQL
- BigQuery
- OpenSky API
- Open-Meteo API
- Git / GitHub
- VS Code

## ES — Estructura del repositorio

- `data/seeds/`: datos semilla controlados, como la dimensión de aeropuertos
- `src/extract/`: clientes y lógica de extracción desde OpenSky y Open-Meteo
- `src/transform/`: transformaciones desde raw a staging, marts y dataset published
- `src/load/`: carga del dataset final a BigQuery
- `src/quality/`: validaciones básicas de calidad de datos
- `src/run_airport_pipeline.py`: runner principal del pipeline end-to-end
- `sql/`: espacio para consultas analíticas y SQL de consumo
- `docs/`: documentación técnica del proyecto
- `scripts/`: scripts auxiliares y pruebas exploratorias fuera del paquete principal
- `data/`: capas locales del pipeline (seed, raw, staging, marts, published)

## ES — Alcance actual

El pipeline actual soporta:
- aeropuertos de España incluidos en el seed local
- procesamiento batch por aeropuerto y fecha
- llegadas y salidas observadas desde OpenSky
- clima horario desde Open-Meteo
- consolidación de resultados multi-airport
- checks básicos de calidad sobre el dataset final publicado
- carga del dataset final consolidado a BigQuery

## ES — Flujo del pipeline

1. lectura del seed de aeropuertos
2. extracción raw de llegadas y salidas desde OpenSky
3. extracción raw de clima desde Open-Meteo
4. transformación a tablas staging
5. construcción de `airport_hourly_operations`
6. enriquecimiento con clima en `airport_hourly_operations_enriched`
7. publicación de un dataset consolidado
8. validación de calidad de datos
9. carga del dataset final a BigQuery

## ES — Dataset final

El output final publicado es un dataset consolidado a nivel de:
- aeropuerto
- hora UTC

Incluye:
- llegadas observadas
- salidas observadas
- vuelos totales observados
- métricas meteorológicas horarias
- flags derivados como:
  - `is_rainy_hour`
  - `is_high_wind_hour`
  - `is_high_traffic_hour`

## ES — Ejecución

Ejemplo de ejecución del pipeline completo:

    python -m src.run_airport_pipeline --airport-icao LEMD --date 2026-03-07

Publicación consolidada:

    python -m src.transform.publish_airport_operations

Quality checks:

    python -m src.quality.check_airport_operations

Carga a BigQuery:

    python -m src.load.bigquery_loader

## ES — Documentación técnica

Documentación adicional disponible en:

- `docs/architecture.md`
- `docs/source_assumptions.md`

## ES — Limitaciones actuales

- el pipeline es batch, no real-time
- OpenSky modela actividad observada por la red, no horarios oficiales exactos
- los quality checks actuales son básicos

---

## EN — Description

End-to-end data engineering project to extract, transform, enrich, and publish observed airport operations and hourly weather data using Python, OpenSky, Open-Meteo, and BigQuery.

The project builds a batch mini data system that:
- ingests observed airport arrivals and departures
- enriches operational activity with hourly weather data
- publishes a consolidated final dataset ready for analysis
- runs basic data quality validations
- loads the final dataset into BigQuery

## EN — Project goal

Build a reproducible and parameterized pipeline that transforms raw flight and weather data into analytical airport-hour tables.

## EN — What this project demonstrates

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

## EN — Stack

- Python
- pandas
- SQL
- BigQuery
- OpenSky API
- Open-Meteo API
- Git / GitHub
- VS Code

## EN — Repository structure

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

## EN — Current scope

The current pipeline supports:
- Spanish airports included in the local seed
- batch processing by airport and date
- observed arrivals and departures from OpenSky
- hourly weather data from Open-Meteo
- multi-airport result consolidation
- basic quality checks on the final published dataset
- loading the final consolidated dataset into BigQuery

## EN — Pipeline flow

1. airport seed loading
2. raw arrivals and departures extraction from OpenSky
3. raw weather extraction from Open-Meteo
4. transformation into staging tables
5. construction of `airport_hourly_operations`
6. weather enrichment in `airport_hourly_operations_enriched`
7. publication of a consolidated dataset
8. data quality validation
9. loading the final dataset into BigQuery

## EN — Final dataset

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

## EN — Run instructions

Example full pipeline run:

    python -m src.run_airport_pipeline --airport-icao LEMD --date 2026-03-07

Consolidated publishing:

    python -m src.transform.publish_airport_operations

Quality checks:

    python -m src.quality.check_airport_operations

BigQuery loading:

    python -m src.load.bigquery_loader

## EN — Technical documentation

Additional documentation is available in:

- `docs/architecture.md`
- `docs/source_assumptions.md`

## EN — Current limitations

- the pipeline is batch-based, not real-time
- OpenSky models network-observed activity, not exact official schedules
- current data quality checks are basic
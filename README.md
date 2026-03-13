# FlightOps Data Platform / Plataforma de Datos de Operaciones Aeroportuarias

## ES — Descripción

Proyecto end-to-end de data engineering para extraer, transformar, enriquecer y publicar datos de operaciones aeroportuarias observadas y meteorología horaria usando Python, OpenSky y Open-Meteo.

El proyecto construye un mini sistema de datos batch que:
- ingiere llegadas y salidas observadas por aeropuerto
- enriquece la actividad operativa con clima horario
- publica un dataset final consolidado listo para análisis
- ejecuta validaciones básicas de calidad de datos

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

## ES — Stack

- Python
- pandas
- SQL
- BigQuery (objetivo siguiente / siguiente fase)
- OpenSky API
- Open-Meteo API
- Git / GitHub
- VS Code

## ES — Alcance actual

El pipeline actual soporta:
- aeropuertos de España incluidos en el seed local
- procesamiento batch por aeropuerto y fecha
- llegadas y salidas observadas desde OpenSky
- clima horario desde Open-Meteo
- consolidación de resultados multi-airport
- checks básicos de calidad sobre el dataset final publicado

## ES — Flujo del pipeline

1. lectura del seed de aeropuertos
2. extracción raw de llegadas y salidas desde OpenSky
3. extracción raw de clima desde Open-Meteo
4. transformación a tablas staging
5. construcción de `airport_hourly_operations`
6. enriquecimiento con clima en `airport_hourly_operations_enriched`
7. publicación de un dataset consolidado
8. validación de calidad de datos

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

## ES — Documentación técnica

Documentación adicional disponible en:

- `docs/architecture.md`
- `docs/pipeline_flow.md`
- `docs/mvp_scope.md`
- `docs/source_assumptions.md`

## ES — Limitaciones actuales

- el pipeline es batch, no real-time
- OpenSky modela actividad observada por la red, no horarios oficiales exactos
- los quality checks actuales son básicos
- la carga final a BigQuery todavía no está integrada en la versión actual

---

## EN — Description

End-to-end data engineering project to extract, transform, enrich, and publish observed airport operations and hourly weather data using Python, OpenSky, and Open-Meteo.

The project builds a batch mini data system that:
- ingests observed airport arrivals and departures
- enriches operational activity with hourly weather data
- publishes a consolidated final dataset ready for analysis
- runs basic data quality validations

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

## EN — Stack

- Python
- pandas
- SQL
- BigQuery (next target / next phase)
- OpenSky API
- Open-Meteo API
- Git / GitHub
- VS Code

## EN — Current scope

The current pipeline supports:
- Spanish airports included in the local seed
- batch processing by airport and date
- observed arrivals and departures from OpenSky
- hourly weather data from Open-Meteo
- multi-airport result consolidation
- basic quality checks on the final published dataset

## EN — Pipeline flow

1. airport seed loading
2. raw arrivals and departures extraction from OpenSky
3. raw weather extraction from Open-Meteo
4. transformation into staging tables
5. construction of `airport_hourly_operations`
6. weather enrichment in `airport_hourly_operations_enriched`
7. publication of a consolidated dataset
8. data quality validation

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

## EN — Technical documentation

Additional documentation is available in:

- `docs/architecture.md`
- `docs/pipeline_flow.md`
- `docs/mvp_scope.md`
- `docs/source_assumptions.md`

## EN — Current limitations

- the pipeline is batch-based, not real-time
- OpenSky models network-observed activity, not exact official schedules
- current data quality checks are basic
- final BigQuery loading is not yet integrated in the current version
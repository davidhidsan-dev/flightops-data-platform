# FlightOps Airport Operations Data Pipeline

English version of this README: [README_EN.md](README_EN.md)

## Descripción

Proyecto end-to-end de data engineering para extraer, transformar, enriquecer y publicar datos de operaciones aeroportuarias observadas y meteorología horaria usando Python, OpenSky, Open-Meteo y BigQuery.

El proyecto construye un mini sistema de datos batch que:
- ingiere llegadas y salidas observadas por aeropuerto
- enriquece la actividad operativa con clima horario
- publica un dataset final consolidado listo para análisis
- ejecuta validaciones básicas de calidad de datos
- carga el dataset final en BigQuery

## Objetivo del proyecto

Construir un pipeline reproducible y parametrizable que transforme datos raw de vuelos y clima en tablas analíticas a nivel de aeropuerto y hora.

## Qué demuestra este proyecto

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

## Stack

- Python
- pandas
- SQL
- BigQuery
- OpenSky API
- Open-Meteo API
- Git / GitHub
- VS Code

## Estructura del repositorio

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

## Alcance actual

El pipeline actual soporta:
- aeropuertos de España incluidos en el seed local
- procesamiento batch por aeropuerto y fecha
- llegadas y salidas observadas desde OpenSky
- clima horario desde Open-Meteo
- consolidación de resultados multi-airport
- checks básicos de calidad sobre el dataset final publicado
- carga del dataset final consolidado a BigQuery

## Flujo del pipeline

1. lectura del seed de aeropuertos
2. extracción raw de llegadas y salidas desde OpenSky
3. extracción raw de clima desde Open-Meteo
4. transformación a tablas staging
5. construcción de `airport_hourly_operations`
6. enriquecimiento con clima en `airport_hourly_operations_enriched`
7. publicación de un dataset consolidado
8. validación de calidad de datos
9. carga del dataset final a BigQuery

## Dataset final

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

## Ejecución

Ejemplo de ejecución del pipeline completo:

    python -m src.run_airport_pipeline --airport-icao LEMD --date 2026-03-07

Este comando ejecuta de extremo a extremo:
- extracción raw
- transformaciones staging
- construcción de marts
- publicación del dataset consolidado
- quality checks
- carga final a BigQuery

Los siguientes comandos pueden ejecutarse de forma independiente solo para desarrollo, depuración o reprocesado manual:

Publicación consolidada:

    python -m src.transform.publish_airport_operations

Quality checks:

    python -m src.quality.check_airport_operations

Carga a BigQuery:

    python -m src.load.bigquery_loader

## Documentación técnica

Documentación adicional disponible en:

- `docs/architecture.md`
- `docs/source_assumptions.md`

## Limitaciones actuales

- el pipeline es batch, no real-time
- OpenSky modela actividad observada por la red, no horarios oficiales exactos
- los quality checks actuales son básicos
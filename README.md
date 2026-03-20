# FlightOps Airport Operations Data Pipeline

English version of this README: [README_EN.md](README_EN.md)

## Descripción

Proyecto end-to-end de data engineering para extraer, transformar, enriquecer y publicar datos de operaciones aeroportuarias observadas y meteorología horaria usando Python, OpenSky, Open-Meteo y BigQuery.

El resultado final es un dataset consolidado a nivel aeropuerto-hora, listo para análisis o carga opcional en BigQuery.

## Objetivo del proyecto

Construir un pipeline reproducible y parametrizable para transformar datos raw de vuelos y clima en un dataset analítico a nivel de aeropuerto-hora.

## Qué demuestra este proyecto

- extracción de datos desde APIs externas
- almacenamiento de datos raw en JSON
- transformaciones desde raw a staging en Python
- construcción de tablas analíticas
- enriquecimiento multi-fuente
- publicación de un dataset consolidado
- validaciones básicas de calidad de datos
- parametrización por aeropuerto y fecha
- ejecución end-to-end mediante un runner
- logging estructurado y retry básico en llamadas a APIs
- carga opcional a BigQuery

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

## Flujo del pipeline

1. lectura del seed de aeropuertos
2. extracción raw de llegadas y salidas desde OpenSky
3. extracción raw de clima desde Open-Meteo
4. transformación a tablas staging
5. construcción de `airport_hourly_operations`
6. enriquecimiento con clima en `airport_hourly_operations_enriched`
7. consolidación del dataset final
8. validación de calidad de datos
9. carga opcional del dataset final a BigQuery

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
- consolidación del dataset final
- quality checks
- carga opcional a BigQuery

Durante la ejecución:
- el pipeline registra logs estructurados en consola
- los clientes API aplican reintentos básicos ante fallos temporales
- si una fuente operativa devuelve resultados vacíos, se genera un warning
- la carga a BigQuery requiere confirmación manual
- si el run contiene warnings, se solicita una confirmación adicional antes de cargar

Los siguientes comandos pueden ejecutarse de forma independiente solo para desarrollo, depuración o reprocesado manual:

Publicación consolidada:

    python -m src.transform.publish_airport_operations

Quality checks:

    python -m src.quality.check_airport_operations

Carga a BigQuery:

    python -m src.load.bigquery_loader

## Documentación técnica

Documentación adicional disponible en:

- [Documentación de arquitectura](docs/architecture.md)
- [Supuestos de la fuente](docs/source_assumptions.md)

## Nota sobre el desarrollo

Este proyecto se desarrolló con apoyo de herramientas de IA como asistencia de programación para acelerar tareas de implementación, refactorización y documentación.

El alcance, la estructura del pipeline, las decisiones de modelado, la validación de supuestos y la revisión final del código fueron realizadas manualmente.

## Limitaciones actuales

- el pipeline es batch, no real-time
- OpenSky modela actividad observada por la red, no horarios oficiales exactos
- los quality checks actuales son básicos
- una respuesta vacía de una fuente operativa puede generar un run potencialmente incompleto, aunque el pipeline deja warnings y confirmación adicional antes de cargar a BigQuery
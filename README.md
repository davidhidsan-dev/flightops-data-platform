# FlightOps Airport Operations Data Pipeline

English version of this README: [README_EN.md](README_EN.md)

> Primer pipeline batch basado en APIs reales, enfocado en extracción, normalización, enriquecimiento y carga opcional a BigQuery.

## Descripción

Proyecto end-to-end de data engineering batch para extraer operaciones aeroportuarias observadas desde OpenSky y meteorología horaria desde Open-Meteo, transformar los datos con Python y preparar una capa analítica reproducible.

El resultado final es un dataset consolidado a nivel aeropuerto-hora con actividad observada, métricas meteorológicas y flags derivados, listo para análisis local o carga opcional en BigQuery.

## Estado del proyecto

Versión funcional completada:

- extracción desde OpenSky y Open-Meteo
- persistencia raw en JSON
- transformaciones staging en Python
- construcción de dataset aeropuerto-hora
- enriquecimiento con meteorología horaria
- quality checks básicos
- logging estructurado y retry básico
- carga opcional a BigQuery

## Qué demuestra este proyecto

- extracción desde APIs externas
- persistencia raw en JSON
- transformaciones a capas staging/marts en Python
- enriquecimiento multi-fuente con meteorología
- dataset consolidado aeropuerto-hora
- quality checks básicos
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
2. extracción desde OpenSky y Open-Meteo
3. persistencia raw en JSON
4. transformaciones staging/marts en Python
5. publicación del dataset aeropuerto-hora
6. quality checks básicos
7. carga opcional a BigQuery

## Dataset final

El output final publicado es un dataset consolidado a nivel aeropuerto-hora UTC.

Incluye llegadas observadas, salidas observadas, vuelos totales observados, métricas meteorológicas horarias y flags derivados como `is_rainy_hour`, `is_high_wind_hour` e `is_high_traffic_hour`.

## Ejecución

Ejemplo de ejecución del pipeline completo:

```bash
python -m src.run_airport_pipeline --airport-icao LEMD --date 2026-03-07
```

Este comando ejecuta extracción raw, transformaciones, publicación del dataset, quality checks y carga opcional a BigQuery.

Durante la ejecución:
- el pipeline registra logs estructurados en consola
- los clientes API aplican reintentos básicos ante fallos temporales
- si una fuente operativa devuelve resultados vacíos, se genera un warning
- la carga a BigQuery requiere confirmación manual
- si el run contiene warnings, se solicita una confirmación adicional antes de cargar

Comandos independientes para desarrollo, depuración o reprocesado manual:

Publicación consolidada:

```bash
python -m src.transform.publish_airport_operations
```

Quality checks:

```bash
python -m src.quality.check_airport_operations
```

Carga a BigQuery:

```bash
python -m src.load.bigquery_loader
```

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
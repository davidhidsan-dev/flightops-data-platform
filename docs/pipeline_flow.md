# Pipeline Flow / Flujo del Pipeline

## ES — Objetivo

Definir el flujo lógico del pipeline de datos desde la extracción hasta la publicación de la tabla final analítica.

## ES — Flujo end-to-end

### 1. Load airport seed
Leer el archivo `data/seeds/dim_airport_seed.csv` para obtener la lista de aeropuertos a monitorizar.

### 2. Extract flight data from OpenSky
Para cada aeropuerto del seed, consultar los endpoints de llegadas y salidas de OpenSky para una ventana temporal definida.

### 3. Extract weather data from Open-Meteo
Para cada aeropuerto, consultar Open-Meteo usando latitud y longitud para obtener variables meteorológicas por hora.

### 4. Store raw data
Guardar las respuestas originales de OpenSky y Open-Meteo en una capa raw para trazabilidad y posibles reprocesados.

### 5. Transform into analytics-ready tables
Transformar los datos raw en tablas limpias y agregadas:
- `airport_hourly_operations`
- `airport_hourly_weather`

### 6. Publish final enriched table
Unir operaciones y clima para generar:
- `airport_hourly_operations_enriched`

## ES — Inputs

- `dim_airport_seed.csv`
- OpenSky Arrivals API
- OpenSky Departures API
- Open-Meteo API

## ES — Outputs

- raw flight data
- raw weather data
- `airport_hourly_operations`
- `airport_hourly_weather`
- `airport_hourly_operations_enriched`

## ES — Primera versión del pipeline

La primera versión del proyecto será batch, no en tiempo real.
Trabajará con un número pequeño de aeropuertos y una ventana temporal acotada.

---

## EN — Objective

Define the logical pipeline flow from data extraction to publication of the final analytical table.

## EN — End-to-end flow

### 1. Load airport seed
Read `data/seeds/dim_airport_seed.csv` to get the list of airports to monitor.

### 2. Extract flight data from OpenSky
For each airport in the seed file, query OpenSky arrivals and departures endpoints for a defined time window.

### 3. Extract weather data from Open-Meteo
For each airport, query Open-Meteo using latitude and longitude to retrieve hourly weather variables.

### 4. Store raw data
Store original OpenSky and Open-Meteo responses in a raw layer for traceability and possible reprocessing.

### 5. Transform into analytics-ready tables
Transform raw data into clean and aggregated tables:
- `airport_hourly_operations`
- `airport_hourly_weather`

### 6. Publish final enriched table
Join operations and weather to generate:
- `airport_hourly_operations_enriched`

## EN — Inputs

- `dim_airport_seed.csv`
- OpenSky Arrivals API
- OpenSky Departures API
- Open-Meteo API

## EN — Outputs

- raw flight data
- raw weather data
- `airport_hourly_operations`
- `airport_hourly_weather`
- `airport_hourly_operations_enriched`

## EN — First pipeline version

The first version of the project will be batch-based, not real-time.
It will work with a small airport set and a limited time window.
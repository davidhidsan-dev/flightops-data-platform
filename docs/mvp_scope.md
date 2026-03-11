# MVP Scope / Alcance del MVP

## ES — Objetivo

Definir el alcance inicial del MVP para construir la primera versión funcional del pipeline.

## ES — Alcance inicial

La primera versión del proyecto trabajará con:
- 3 aeropuertos de España:
  - LEMD
  - LEBL
  - LEPA
- una ventana temporal de 1 día completo
- timestamps en UTC
- datos de llegadas y salidas desde OpenSky
- datos meteorológicos por hora desde Open-Meteo

## ES — Qué incluye el MVP

- lectura del seed de aeropuertos
- extracción de llegadas y salidas
- extracción de clima
- almacenamiento raw local en JSON
- primera transformación a tablas analíticas
- tabla final enriquecida por aeropuerto y hora

## ES — Qué queda fuera por ahora

- tiempo real
- más aeropuertos
- automatización programada
- data quality checks avanzados
- despliegue productivo

---

## EN — Objective

Define the initial MVP scope for building the first functional version of the pipeline.

## EN — Initial scope

The first version of the project will work with:
- 3 Spanish airports:
  - LEMD
  - LEBL
  - LEPA
- a 1-day time window
- UTC timestamps
- arrivals and departures data from OpenSky
- hourly weather data from Open-Meteo

## EN — What the MVP includes

- airport seed loading
- arrivals and departures extraction
- weather extraction
- local raw JSON storage
- first transformation into analytical tables
- final airport-hour enriched table

## EN — Out of scope for now

- real-time processing
- more airports
- scheduled automation
- advanced data quality checks
- production deployment
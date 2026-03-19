# Source Assumptions / Supuestos de la Fuente

## ES — Uso de firstSeen y lastSeen

OpenSky no garantiza que `firstSeen` y `lastSeen` representen exactamente la hora real de salida o llegada.

Según la documentación de OpenSky:
- `firstSeen` puede representar el primer punto observado después de la salida esperada o el momento en que la aeronave entra en el rango de recepción de la red.
- `lastSeen` puede representar el último punto observado antes de la llegada esperada o el momento en que la aeronave sale del rango de recepción de la red.

Por tanto, en este proyecto:
- `firstSeen` se utiliza como proxy de tiempo de salida observada
- `lastSeen` se utiliza como proxy de tiempo de llegada observada

Estas variables se interpretan como timestamps operativos observados por la red, no como tiempos exactos de operación aeroportuaria.

## ES — Respuestas vacías y cobertura de la fuente

Las APIs operativas pueden devolver respuestas vacías para una fecha, aeropuerto o ventana temporal concreta.

En este proyecto:
- una respuesta vacía no se interpreta automáticamente como error técnico
- pero sí puede indicar cobertura incompleta, ausencia temporal de datos o limitaciones de la fuente
- por ese motivo, el pipeline registra warnings cuando una fuente operativa devuelve cero registros

Esto significa que un run puede completarse técnicamente y, aun así, requerir cautela en la interpretación del output si alguna fuente operativa ha venido vacía.

## ES — Implicaciones para el modelo

El dataset final modela movimientos observados de vuelos por aeropuerto y hora.

Esto significa que:
- las métricas representan actividad observada
- no deben interpretarse como horarios oficiales exactos
- pueden existir sesgos de cobertura según la red de recepción
- una ausencia de datos en una fuente no implica necesariamente ausencia real de operaciones

---

## EN — Use of firstSeen and lastSeen

OpenSky does not guarantee that `firstSeen` and `lastSeen` represent the exact real-world departure or arrival time.

According to the OpenSky documentation:
- `firstSeen` may represent the first observed point after the expected departure or the moment the aircraft enters the network reception range.
- `lastSeen` may represent the last observed point before the expected arrival or the moment the aircraft leaves the network reception range.

Therefore, in this project:
- `firstSeen` is used as a proxy for observed departure time
- `lastSeen` is used as a proxy for observed arrival time

These variables are interpreted as network-observed operational timestamps, not exact airport operation timestamps.

## EN — Empty responses and source coverage

Operational APIs may return empty responses for a given date, airport, or time window.

In this project:
- an empty response is not automatically interpreted as a technical failure
- but it may indicate incomplete coverage, temporary lack of available data, or source limitations
- for that reason, the pipeline logs warnings when an operational source returns zero records

This means that a run may complete successfully from a technical perspective while still requiring caution in interpretation if one operational source returned empty data.

## EN — Modeling implications

The final dataset models observed flight movements by airport and hour.

This means that:
- metrics represent observed activity
- they should not be interpreted as exact official schedules
- coverage bias may exist depending on network reception
- a lack of data in one source does not necessarily imply a real lack of operations
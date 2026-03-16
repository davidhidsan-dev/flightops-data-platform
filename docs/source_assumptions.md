# Source Assumptions / Supuestos de la Fuente

## ES — Uso de firstSeen y lastSeen

OpenSky no garantiza que `firstSeen` y `lastSeen` representen exactamente la hora real de salida o llegada.

Según la documentación de OpenSky:
- `firstSeen` puede representar el primer punto observado después de la salida esperada o el momento en que la aeronave entra en el rango de recepción de la red.
- `lastSeen` puede representar el último punto observado antes de la llegada esperada o el momento en que la aeronave sale del rango de recepción de la red.

Por tanto, en este proyecto:
- `firstSeen` se utilizará como proxy de tiempo de salida observada
- `lastSeen` se utilizará como proxy de tiempo de llegada observada

Estas variables se interpretarán como timestamps operativos observados por la red, no como tiempos exactos de operación aeroportuaria.

## ES — Implicaciones para el modelo

El dataset final modela movimientos observados de vuelos por aeropuerto y hora.

Esto significa que:
- las métricas representan actividad observada
- no deben interpretarse como horarios oficiales exactos
- pueden existir sesgos de cobertura según la red de recepción

---

## EN — Use of firstSeen and lastSeen

OpenSky does not guarantee that `firstSeen` and `lastSeen` represent the exact real-world departure or arrival time.

According to the OpenSky documentation:
- `firstSeen` may represent the first observed point after the expected departure or the moment the aircraft enters the network reception range.
- `lastSeen` may represent the last observed point before the expected arrival or the moment the aircraft leaves the network reception range.

Therefore, in this project:
- `firstSeen` will be used as a proxy for observed departure time
- `lastSeen` will be used as a proxy for observed arrival time

These variables will be interpreted as network-observed operational timestamps, not exact airport operation timestamps.

## EN — Modeling implications

The final dataset models observed flight movements by airport and hour.

This means that:
- metrics represent observed activity
- they should not be interpreted as exact official schedules
- coverage bias may exist depending on network reception
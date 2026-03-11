# OpenSky Arrivals Field Selection / Selección de Campos de OpenSky Arrivals

## ES — Campos detectados

- `icao24`
- `firstSeen`
- `estDepartureAirport`
- `lastSeen`
- `estArrivalAirport`
- `callsign`
- `estDepartureAirportHorizDistance`
- `estDepartureAirportVertDistance`
- `estArrivalAirportHorizDistance`
- `estArrivalAirportVertDistance`
- `departureAirportCandidatesCount`
- `arrivalAirportCandidatesCount`

## ES — Campos que usaremos en el MVP

- `icao24`
- `callsign`
- `firstSeen`
- `lastSeen`
- `estDepartureAirport`
- `estArrivalAirport`

## ES — Campos que dejamos fuera por ahora

- `estDepartureAirportHorizDistance`
- `estDepartureAirportVertDistance`
- `estArrivalAirportHorizDistance`
- `estArrivalAirportVertDistance`
- `departureAirportCandidatesCount`
- `arrivalAirportCandidatesCount`

## ES — Justificación

En el MVP, el objetivo principal es construir una primera tabla de movimientos observados por aeropuerto y hora con la menor complejidad posible.

Por ello, se seleccionan únicamente los campos mínimos necesarios para:
- identificar el vuelo o aeronave
- asociar el movimiento a un aeropuerto
- derivar una referencia temporal operativa
- contar vuelos y aeronaves por intervalo horario

Los campos `icao24` y `callsign` permiten identificar aeronaves y vuelos observados.
Los campos `estDepartureAirport` y `estArrivalAirport` permiten asociar el movimiento a aeropuertos concretos.
Los campos `firstSeen` y `lastSeen` permiten derivar proxies temporales para salidas observadas y llegadas observadas, respectivamente.

Los campos de distancia y número de candidatos se excluyen en esta primera versión porque introducen complejidad adicional y no son estrictamente necesarios para construir la tabla analítica principal.
No obstante, pueden ser útiles en una versión posterior para:
- evaluar calidad de la inferencia aeroportuaria
- construir flags de fiabilidad
- filtrar observaciones ambiguas

Por tanto, estos campos se dejan explícitamente fuera del MVP y se reservan para una posible V2 del proyecto.

---

## EN — Detected fields

- `icao24`
- `firstSeen`
- `estDepartureAirport`
- `lastSeen`
- `estArrivalAirport`
- `callsign`
- `estDepartureAirportHorizDistance`
- `estDepartureAirportVertDistance`
- `estArrivalAirportHorizDistance`
- `estArrivalAirportVertDistance`
- `departureAirportCandidatesCount`
- `arrivalAirportCandidatesCount`

## EN — Fields used in the MVP

- `icao24`
- `callsign`
- `firstSeen`
- `lastSeen`
- `estDepartureAirport`
- `estArrivalAirport`

## EN — Fields excluded for now

- `estDepartureAirportHorizDistance`
- `estDepartureAirportVertDistance`
- `estArrivalAirportHorizDistance`
- `estArrivalAirportVertDistance`
- `departureAirportCandidatesCount`
- `arrivalAirportCandidatesCount`

## EN — Justification

In the MVP, the main goal is to build a first observed-movements table by airport and hour with the lowest possible complexity.

For that reason, only the minimum required fields are selected to:
- identify the observed flight or aircraft
- associate the movement with an airport
- derive an operational time reference
- count flights and aircraft by hourly interval

The `icao24` and `callsign` fields make it possible to identify observed aircraft and flights.
The `estDepartureAirport` and `estArrivalAirport` fields allow us to associate movements with specific airports.
The `firstSeen` and `lastSeen` fields provide temporal proxies for observed departures and observed arrivals, respectively.

Distance fields and candidate-count fields are excluded from the first version because they add extra complexity and are not strictly necessary to build the main analytical table.
However, they may be useful in a later version to:
- assess airport inference quality
- build reliability flags
- filter ambiguous observations

Therefore, these fields are explicitly left out of the MVP and reserved for a possible V2 of the project.
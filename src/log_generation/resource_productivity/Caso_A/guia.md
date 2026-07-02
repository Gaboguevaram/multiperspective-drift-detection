# Caso A — Escenario base

## Propósito

Primer escenario exploratorio, con pocos recursos y mezcla heterogénea de familias. Sirve como caso semilla a partir del cual surgieron los demás (B, C, D).

## Configuración del proceso

- **Modelo BPMN**: lineal A → B → C → D → E (5 tareas)
- **Recursos**: 3 (Carlos, Sandra, Pedro), con asignación parcial entre tareas
- **Calendario**: 24/7
- **Llegada de casos**: `norm` media=1800s (~30 min), std=1500s
- **`amount=1`** por recurso

## Distribuciones por par (recurso, tarea)

Todas con `clip_min=30s` para evitar duraciones cercanas a cero.

| Tarea | Recurso | Familia | Parámetros |
|---|---|---|---|
| A | Carlos | `uniform` | rango [150, 400] |
| A | Sandra | `norm` | media=280, std=80 |
| B | Carlos | `expon` | media=250 |
| B | Sandra | `lognorm` | media=320, std=120 |
| C | Carlos | `norm` | media=450, std=150 |
| C | Sandra | `uniform` | rango [200, 550] |
| D | Pedro | `lognorm` | media=600, std=250 |
| D | Sandra | `norm` | media=550, std=200 |
| E | Pedro | `expon` | media=667 |
| E | Sandra | `uniform` | rango [400, 900] |

## Volumen esperado

Con 2500 trazas y 2 recursos por tarea: ~2500 eventos por tarea, ~1250 eventos por par si la asignación es equilibrada.

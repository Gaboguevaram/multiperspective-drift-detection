# Caso C — Volumen y diversidad

## Propósito

Validar que el detector de familia mantiene la **coherencia** cuando hay muchos pares (recurso, tarea) y las familias generadoras son **diversas y se reparten heterogéneamente** entre los recursos. Es la prueba de robustez con volumen alto.

## Configuración del proceso

- **Modelo BPMN**: lineal A → B → C → D → E (5 tareas)
- **Recursos**: 8 distintos (Ana, Bruno, Carla, Diego, Elena, Fer, Gloria, Hugo), **3 por tarea** con solapamiento entre tareas (un mismo recurso puede aparecer en varias tareas con familias distintas)
- **Calendario**: 24/7
- **Llegada de casos**: `expon` media=30s → ~2880 trazas/día (gran volumen)
- **`amount=1`** por recurso

## Distribuciones por par (recurso, tarea)

Todas con `clip_min=30s` para evitar duraciones cercanas a cero.

| Tarea | Recurso | Familia | Parámetros |
|---|---|---|---|
| A | Ana | `norm` | media=200, std=30 |
| A | Bruno | `lognorm` | media=300, std=150 |
| A | Carla | `expon` | media=250 |
| B | Diego | `gamma` | shape=3, scale=100 |
| B | Elena | `uniform` | rango [100, 500] |
| B | Fer | `norm` | media=400, std=80 |
| C | Ana | `expon` | media=400 |
| C | Diego | `lognorm` | media=600, std=250 |
| C | Gloria | `gamma` | shape=2, scale=200 |
| D | Bruno | `uniform` | rango [200, 700] |
| D | Elena | `gamma` | shape=2, scale=200 |
| D | Hugo | `norm` | media=500, std=100 |
| E | Carla | `lognorm` | media=700, std=300 |
| E | Fer | `expon` | media=500 |
| E | Hugo | `norm` | media=600, std=150 |

## Volumen esperado

Con 2500 trazas y 3 recursos por tarea:
- ~2500 eventos por tarea
- ~830 eventos por par (recurso, tarea) si la asignación es equilibrada
- Margen amplio para ajustes paramétricos robustos

## Hipótesis a validar

- Cada par tiene una familia distinta de su "vecino" de tarea, por lo que la identificación correcta es exigente.
- Recursos compartidos entre tareas (Ana, Bruno, Diego, etc.) con **familias distintas en cada tarea** verifican que el detector no contamina ajustes entre pares.

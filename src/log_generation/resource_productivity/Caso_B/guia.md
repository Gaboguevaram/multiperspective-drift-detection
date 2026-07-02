# Caso B — Contraste fuerte por tarea

## Propósito

Validar que el detector de familia de distribución **identifica correctamente** la generadora cuando las dos opciones por tarea son **claramente contrastadas** (una con cola larga vs otra simétrica, etc.). Es la prueba de identificabilidad: si en este escenario el detector falla, el problema está en el algoritmo, no en los datos.

## Configuración del proceso

- **Modelo BPMN**: lineal A → B → C (3 tareas)
- **Recursos**: 6 en total, 2 distintos por tarea (sin solapamiento entre tareas)
- **Calendario**: 24/7 (sin huecos que distorsionen el TPA)
- **Llegada de casos**: `expon` media=60s → ~1440 trazas/día
- **`amount=1`** por recurso: fuerza distribución por disponibilidad real

## Distribuciones por par (recurso, tarea)

Todas con `clip_min=30s` para evitar duraciones cercanas a cero.

| Tarea | Recurso | Familia | Parámetros |
|---|---|---|---|
| A | Carlos | `expon` | media = 300s (cola larga marcada) |
| A | Sandra | `norm` | media = 300s, std = 20s (campana estrecha) |
| B | Pedro | `lognorm` | media = 500s, std = 300s (asimétrica, cola derecha) |
| B | Laura | `uniform` | rango [200, 800] (plana) |
| C | Marta | `gamma` | shape = 2, scale = 150 (asimétrica suave) |
| C | Diego | `norm` | media = 400s, std = 150s (campana ancha) |

## Volumen esperado

Con 2500 trazas y 2 recursos por tarea:
- ~2500 eventos por tarea (uno por traza)
- ~1250 eventos por par (recurso, tarea) si la asignación es equilibrada
- Suficiente para que los ajustes paramétricos sean robustos

## Hipótesis a validar

- **Tarea A**: `expon` (Carlos) y `norm` (Sandra) son fácilmente distinguibles. Esperado: el detector corona la familia correcta en cada par.
- **Tarea B**: `lognorm` (Pedro) y `uniform` (Laura) tienen formas radicalmente distintas. Esperado: identificación clara.
- **Tarea C**: `gamma` (Marta) y `norm` (Diego) son más cercanas en forma; aquí se pone a prueba la sensibilidad real del detector.

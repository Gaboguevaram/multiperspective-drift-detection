# Caso F — Productividad: un recurso, varias tareas (RP-A)

Un único recurso (Carlos) que ejecuta las cinco tareas (A–E). En el cambio (al 50 % del log) se altera la distribución del tiempo de ejecución de **tres tareas (A, C, E)** y se mantienen **dos (B, D)** como control.

## Por qué un solo recurso, y por qué se cambia la dispersión (no la media)

La productividad de un par (recurso, tarea) se calcula como `TPA_evento / TPA_medio_de_la_tarea`, es decir, **se normaliza por la media de la tarea**. Con un único recurso esa media es la del propio Carlos, así que la productividad mide la **dispersión relativa** del tiempo de servicio, **no su media**: cambiar la media se cancela en la normalización y no genera señal. Por eso el cambio se inyecta como un **aumento drástico de la dispersión** (coeficiente de variación), no de la media.

El control es por **tareas del mismo recurso** (B, D): usar varios recursos no serviría, porque la normalización por la media de la tarea contaminaría a los recursos de control.

## Cambios inyectados (base → drift)

Todas las tareas parten muy concentradas (productividad ≈ 1, distancia de referencia ≈ 0):

| Tarea | Base | Drift | ¿Cambia? |
|-------|------|-------|----------|
| A | norm(1000, 20) | norm(1000, 400) | sí (CV 0,02 → 0,4) |
| B | norm(1000, 20) | norm(1000, 20) | no (control) |
| C | norm(1000, 20) | norm(1000, 400) | sí (CV 0,02 → 0,4) |
| D | norm(1000, 20) | norm(1000, 20) | no (control) |
| E | norm(1000, 20) | norm(1000, 400) | sí (CV 0,02 → 0,4) |

## Métrica y ground truth

Distancia de Wasserstein entre la distribución de productividad de referencia de cada par y la re-ajustada por ventana. Deben **subir** (Carlos, A), (Carlos, C) y (Carlos, E), y mantenerse **planas** (Carlos, B) y (Carlos, D). El **ground truth es el par (recurso, tarea)**: la validación del F-score comprueba recurso y tarea.

Corresponde a RP-A en la batería de logs simples. Log: `data/01_raw/resource_productivity/Caso_F/log_productivity_sudden_caso_f_5000.csv`.

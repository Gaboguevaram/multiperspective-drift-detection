# Caso D — Ground truth: misma familia para todos los recursos por tarea

## Propósito

Caso de **control / suelo de verdad**. Todos los recursos (Carlos, Sandra, Pedro) ejecutan **cada tarea con la misma distribución generadora**. Solo la familia cambia entre tareas. Es la prueba canónica de **consistencia** del detector: si los tres recursos hacen la tarea A con la misma `norm(300, 50)`, la familia ganadora debe ser **`norm` los tres**. Cualquier discrepancia entre recursos en la misma tarea es ruido del detector, no del proceso.

## Configuración del proceso

- **Modelo BPMN**: lineal A → B → C → D → E (5 tareas)
- **Recursos**: 3 (Carlos, Sandra, Pedro), **todos asignados a las 5 tareas**
- **Calendario**: 24/7
- **Llegada de casos**: `expon` media=30s → ~2880 trazas/día
- **`amount=1`** por recurso

## Distribuciones por tarea (idénticas para los 3 recursos)

Todas con `clip_min=30s` para evitar duraciones cercanas a cero.

| Tarea | Familia generadora (Carlos = Sandra = Pedro) |
|---|---|
| A | `norm` media=300, std=50 |
| B | `lognorm` media=400, std=150 |
| C | `gamma` shape=2, scale=200 |
| D | `expon` media=300 |
| E | `uniform` rango [200, 600] |

## Volumen esperado

Con 2500 trazas y 3 recursos por tarea:
- ~2500 eventos por tarea
- ~830 eventos por par (recurso, tarea)

## Hipótesis a validar

- **Por tarea**, los 3 pares deben ganar la misma familia. Si no, hay ruido en la selección de AIC.
- **Entre tareas**, debe coronar la familia correcta (norm, lognorm, gamma, expon, uniform respectivamente).
- Es el escenario donde se mide el **piso de error** del detector. Si aquí falla, los Casos A/B/C no se pueden interpretar fiablemente.

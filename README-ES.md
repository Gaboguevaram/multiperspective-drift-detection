# Multiperspective Drift Detector — Detección de Deriva en Procesos Multi-perspectiva

<div align="center">

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![pm4py](https://img.shields.io/badge/pm4py-2.7%2B-orange?style=for-the-badge)
![Prefect](https://img.shields.io/badge/prefect-3.6%2B-024DFD?style=for-the-badge&logo=prefect)
![TFG](https://img.shields.io/badge/TFG-USC-red?style=for-the-badge)

**Herramienta académica para la detección de *concept drift* en logs de eventos mediante análisis multi-perspectiva y minería de procesos.**

[Descripción](#-descripción) · [Tecnologías](#-tecnologías-utilizadas) · [Instalación](#-instalación) · [Estructura](#-estructura-del-proyecto) · [Configuración](#-configuración-de-parámetros) · [Perspectivas](#-perspectivas-disponibles) · [Uso](#-uso)
</div>

---

## 📋 Descripción

La **Minería de Procesos** (*Process Mining*) permite a las organizaciones obtener una visión objetiva de sus operaciones a partir de los registros de eventos. Por ende, la eficacia de estos procesos se ve comprometida por el **concept drift**: la evolución de las propiedades del proceso a lo largo del tiempo. Pese a su importancia, las aproximaciones actuales se limitan mayoritariamente a la detección de cambios en el flujo de control (**control-flow**), ignorando otras dimensiones críticas como el rendimiento de los recursos o los tiempos de ciclo, que resultan vitales para la competitividad empresarial.

Este trabajo propone el diseño e implementación de un framework de **Detección de Cambios Multiperspectiva** (*Multiperspective Drift Detection*) que no solo identifique derivas en diferentes dimensiones, sino que también analice las relaciones de causalidad entre ellas. La metodología se basa en el modelado de las distintas perspectivas mediante técnicas de machine learning y la monitorización de estas a través de métricas específicas; de forma que una degradación en el rendimiento del modelo servirá como indicador de que se produjo un cambio en el proceso. El objetivo final es ofrecer una visión integral que permita correlacionar las variaciones detectadas —por ejemplo, si un cambio en el flujo de control es el causante directo de un incremento en el tiempo de ciclo—, facilitando así un diagnóstico más profundo y certero de la evolución del proceso.

### Características principales

| Característica | Descripción |
|---|---|
| **3 modos de ventana** | `temporal` (por rango de fechas), `eventos` (por número de eventos) y `trazas` (por número de trazas completas) |
| **Multi-perspectiva** | Ejecución simultánea e independiente de múltiples perspectivas de análisis sobre el mismo log |
| **5 perspectivas integradas** | `control_flow` (Redes de Petri), `arrival_rate` (tasa de llegada), `service_rate` (tasa de servicio), `resource_profiles` (perfiles de recursos) y `calendar` (calendarios de recursos) |
| **Arquitectura extensible** | Sistema de registro (*registry pattern*) para añadir nuevos filtros, transformaciones, modelos y métricas sin modificar el núcleo |
| **Configuración YAML** | Toda la ejecución se parametriza a través de ficheros YAML, sin tocar el código fuente |
| **Orquestación con Prefect** | Flujos de trabajo robustos con reintentos automáticos, trazabilidad y paralelización de tareas |
| **Detección de drift configurable** | Dos algoritmos: `deteccion_regresion` (regresión lineal sobre cualquier métrica escalar) y `deteccion_distribucion` (pertenencia a distribución de referencia por par recurso-tarea) |

### Contexto académico

Este proyecto constituye el **Trabajo de Fin de Grado (TFG)** de Gabriel Guevara Muradás, desarrollado en la **Universidade de Santiago de Compostela (USC)**. Su objetivo es proporcionar una herramienta flexible, reproducible y extensible para la investigación en detección de deriva en procesos, aplicable tanto en entornos académicos como en casos de uso industriales reales.

---

## Visuales

> **📌 Placeholder de capturas y demostraciones**
>
> *A continuación se incluirán GIFs y capturas de pantalla mostrando la herramienta en funcionamiento: ejecución del pipeline, visualización de Redes de Petri descubiertas, métricas por ventana y detección de drift.*

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│   [ GIF: Ejecución del pipeline multi-perspectiva ]            │
│                                                                 │
│   [ Captura: Red de Petri descubierta con Inductive Miner ]    │
│                                                                 │
│   [ Gráfica: Evolución de fitness/precision por ventana ]      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tecnologías Utilizadas

### Core

| Tecnología | Versión | Rol en el proyecto |
|---|---|---|
| **Python** | ≥ 3.10 | Lenguaje principal del proyecto |
| **[pm4py](https://pm4py.fit.fraunhofer.de/)** | ≥ 2.7.19 | Librería de Process Mining: lectura de logs XES, descubrimiento de modelos (Inductive Miner, Heuristic Miner), cálculo de métricas de conformidad (fitness, precision) y filtrado temporal de eventos mediante `filter_time_range` |
| **[pix-framework](https://github.com/AutomatedProcessImprovement/pix-framework)** | ~ 0.13.17 | Descubrimiento *fuzzy* de calendarios de recursos (`discovery_fuzzy_resource_calendars_and_performances`) y utilidades estándar de identificadores de log de eventos (`EventLogIDs`, `DEFAULT_XES_IDS`, `read_csv_log`) reutilizadas en las perspectivas de calendar, resource_profiles, arrival_rate y service_rate |
| **[Prefect](https://www.prefect.io/)** | ≥ 3.6.19 | Orquestación de flujos de trabajo: gestión del ciclo de vida de las tareas, reintentos automáticos (`retries`), paralelización de perspectivas mediante `submit()` y trazabilidad completa de las ejecuciones |
| **[pandas](https://pandas.pydata.org/)** | ≥ 2.3.3 | Manipulación y transformación de los DataFrames que representan los logs de eventos |
| **[PyYAML](https://pyyaml.org/)** | ≥ 6.0.3 | Carga y validación de los ficheros de configuración de parámetros |

### Machine Learning y análisis

| Tecnología | Rol en el proyecto |
|---|---|
| **[scikit-learn](https://scikit-learn.org/)** | `RandomForestRegressor` para modelar arrival/service rate; `TimeSeriesSplit` para validación cruzada temporal; `RandomizedSearchCV` / `GridSearchCV` para optimización de hiperparámetros; `VarianceThreshold` para selección de features; `mean_absolute_error` / `mean_squared_error` para el cálculo de MAE y MSE |
| **[NumPy](https://numpy.org/)** | Operaciones vectoriales y de álgebra para el cálculo de métricas (entropía circadiana, distribuciones de recursos, distancias TV) y para los comparadores del auto-ajuste de ventana |
| **[scipy](https://scipy.org/)** | `linregress` para las regresiones lineales de los algoritmos de detección de drift y `scipy.stats` para los tests estadísticos (Kolmogorov–Smirnov) usados en el comparador del auto-ajuste de ventana |
| **[seaborn](https://seaborn.pydata.org/)** | Visualización de métricas y resultados |
| **[Graphviz](https://graphviz.org/)** | Renderizado de los grafos de Redes de Petri descubiertas |

### Generación de logs sintéticos

| Tecnología | Versión | Rol en el proyecto |
|---|---|---|
| **[ProSimos](https://github.com/AutomatedProcessImprovement/Prosimos)** | ~ 2.0.6 | Simulador de procesos de negocio. Se invoca como CLI desde los scripts de `src/log_generation/` para generar los logs sintéticos (con y sin drift) que se utilizan en las pruebas controladas de cada perspectiva: arrival rate, calendar y los cuatro casos de resource_productivity |

### Entorno

| Tecnología | Rol en el proyecto |
|---|---|
| **[Conda](https://docs.conda.io/)** | Gestor de entornos y dependencias utilizado durante el desarrollo del TFG. El entorno de trabajo (`tfg-mineria`) aísla la versión exacta de Python (3.10.19) y las versiones fijadas de pm4py, pix-framework, ProSimos, scikit-learn y demás librerías declaradas en `pyproject.toml`, evitando conflictos con otras instalaciones del sistema y garantizando ejecuciones reproducibles |

---

## Instalación

### Prerrequisitos

- **Conda** ([Miniconda](https://docs.conda.io/projects/miniconda/) o [Anaconda](https://www.anaconda.com/download)) — gestor de entornos usado durante el desarrollo del TFG. El proyecto fija `python==3.10.19` en `pyproject.toml`, por lo que conda se encarga de instalar exactamente esa versión dentro del entorno aislado
- **Graphviz** (necesario para la visualización de Redes de Petri):
  - macOS: `brew install graphviz`
  - Ubuntu/Debian: `sudo apt-get install graphviz`
  - Windows: Descarga del instalador desde [graphviz.org](https://graphviz.org/download/)

### Pasos de instalación

**1. Clona el repositorio**

```bash
git clone https://github.com/<tu-usuario>/multiperspective-drift-detector.git
cd multiperspective-drift-detector
```

**2. Crea y activa el entorno conda**

```bash
conda create -n tfg-mineria python=3.10.19 -y
conda activate tfg-mineria
```

> 💡 El nombre del entorno (`tfg-mineria`) es una convención; puedes usar cualquier otro pasando `-n <nombre>` en el comando anterior. El resto de la guía asume `tfg-mineria`.

**3. Instala las dependencias del proyecto**

Dentro del entorno activo, instala el paquete en modo editable junto con todas las dependencias declaradas en `pyproject.toml` (pm4py, pix-framework, ProSimos, scikit-learn, pandas, etc.):

```bash
pip install -e .
```

TODO: creo q 4 no es necesario

**4. Prepara los directorios de trabajo**

```bash
mkdir -p data/01_raw data/06_models logs
```

TODO: ESTA MAL ESOT
**5. Coloca tu log de eventos en `data/01_raw/`**

El sistema acepta logs en formato **`.xes`** (para perspectivas de control-flow) y **`.csv`** (para perspectivas de arrival/service rate):

```bash
cp /ruta/a/tu/log.xes data/01_raw/
```

---

## 🗂️ Estructura del Proyecto

El proyecto sigue una estructura modular y estandarizada, inspirada en las convenciones de proyectos de Data Science de referencia (como [Kedro](https://kedro.org/)):

```
multiperspective-drift-detector/
│
├── conf/                              # Configuración de ejecuciones
│   ├── __init__.py
│   ├── parameters.yml                 # Config principal (ventana temporal + control-flow)
│   ├── parameters_lineal.yml          # Variante experimental con progresión lineal
│   ├── ventana_trazas.yml             # Config ventana por trazas + detección por regresión
│   ├── ventana_eventos.yml            # Config ventana por número de eventos
│   ├── arrival_rate.yml               # Config perspectiva arrival rate (ML)
│   ├── service_rate.yml               # Config perspectiva service rate (ML)
│   └── pruebas/                       # Ficheros de prueba y experimentación
│       └── ventana_eventos.yml
│
├── data/                              # Datos del proyecto (estándar por capas)
│   ├── 01_raw/                        # Logs de entrada originales (.xes, .csv) — SOLO LECTURA
│   ├── 02_intermediate/               # Datos intermedios del pipeline
│   ├── 03_primary/                    # Datos primarios transformados
│   ├── 04_feature/                    # Features extraídas para los modelos ML
│   ├── 05_model_input/                # Datos listos para alimentar los modelos
│   ├── 06_models/                     # Modelos descubiertos: Redes de Petri (.png)
│   ├── 07_model_output/               # Salidas y predicciones de los modelos
│   └── 08_reporting/                  # Informes y visualizaciones finales
│
├── Docs/                              # Documentación del proyecto
│
├── src/                               # Código fuente principal
│   ├── __init__.py
│   ├── config.py                      # Carga y validación de ficheros YAML
│   ├── convertidor.py                 # Utilidades de conversión de formatos de log
│   ├── main_flow.py                   # Punto de entrada: orquestador Prefect
│   ├── settings.py                    # Constantes y valores por defecto globales
│   ├── registro_op.py                 # Registros de operaciones (registry pattern)
│   ├── metricas.py                    # Implementación de todas las métricas
│   ├── ventana.py                     # Lógica de extracción y avance de los 3 modos de ventana
│   ├── concept_drift_detection.py     # Algoritmos de detección: deteccion_regresion y deteccion_distribucion
│   ├── logging_config.py              # Configuración centralizada del sistema de logging
│   └── perspectivas/                  # Módulos por perspectiva de análisis
│       ├── control_flow.py            # Filtros, transformaciones y modelos de control-flow
│       ├── arrival_rate.py            # Pipeline completo arrival rate (preprocesado + ML)
│       └── service_rate.py            # Pipeline completo service rate (preprocesado + ML)
│
└── tests/                             # Suite de tests
```

### Responsabilidades por módulo

| Módulo | Descripción |
|---|---|
| `conf/` | Contiene todos los ficheros YAML que parametrizan las ejecuciones. Es el único lugar a modificar para cambiar el comportamiento sin tocar el código |
| `src/main_flow.py` | Orquestador principal. Define los flujos Prefect `orquestador_multidimensional` y `lanzar_iteracion`, y gestiona el ciclo de vida completo del pipeline |
| `src/registro_op.py` | Centraliza los cinco registros del sistema: `REGISTRO_FILTRADO`, `REGISTRO_TRANSFORMACIONES`, `REGISTRO_MODELOS`, `REGISTRO_METRICAS` y `REGISTRO_DETECCION`. Es el único fichero a modificar para registrar nuevas operaciones |
| `src/ventana.py` | Implementa los tres modos de ventana (`temporal`, `eventos`, `trazas`), su extracción inicial, el avance iterativo y la detección de entrada de nuevas trazas (`entro_traza_nueva`) |
| `src/metricas.py` | Implementación de `fitness`, `precision`, `MAE` y `MSE`. Despacha al cálculo correcto según si la perspectiva es de control-flow o ML |
| `src/concept_drift_detection.py` | Implementación de los algoritmos de detección: `deteccion_regresion` (regresión lineal genérica sobre cualquier métrica escalar, inspirado en C2D2) y `deteccion_distribucion` (racha de pertenencia a distribución por par recurso-tarea) |
| `src/perspectivas/control_flow.py` | Filtrado de trazas completas, transformaciones y descubrimiento de modelos. Calcula automáticamente el **OLP** (*One-Length Paths*) mediante simulación con `pm4py.play_out` |
| `src/perspectivas/arrival_rate.py` | Pipeline completo: filtrado de eventos de inicio, cálculo de tasa de llegada por sub-ventana, codificación cíclica de variables temporales, lag features y entrenamiento con `RandomizedSearchCV` |
| `src/perspectivas/service_rate.py` | Pipeline análogo al de arrival rate para la tasa de finalización de trazas, con `GridSearchCV` y `VarianceThreshold` |
| `src/config.py` | Carga, parseo y validación de los ficheros YAML de `conf/` |
| `src/logging_config.py` | Configuración centralizada del sistema de logging, utilizado por todos los módulos vía `get_logger(__name__)` |

---

## ⚙️ Configuración de Parámetros

Toda la ejecución se parametriza mediante ficheros YAML en `conf/`. La configuración se divide en dos bloques: `configuracion_global` y la lista `perspectivas`.

---

### Modos de ventana

El bloque `ventana` admite **tres modos de operación excluyentes**:

| Modo | Tipo de corte | Caso de uso recomendado |
|---|---|---|
| `temporal` | Por rango de fechas | Logs con timestamps reales; análisis de patrones estacionales |
| `eventos` | Por número de eventos | Logs sin timestamps fiables; control preciso del tamaño de la ventana |
| `trazas` | Por número de trazas completas | Cuando la unidad de análisis debe ser siempre una traza íntegra |

#### Ventanas independientes por perspectiva

A partir de la versión actual, es posible asignar **ventanas diferentes a cada perspectiva**, permitiendo que perspectivas con horizontes temporales muy distintos (ej. control flow cada 1 día vs. calendar cada 90 días) se ejecuten sin que una espere innecesariamente a la otra.

**Modos de asignación:**

1. **Uni-ventana (legacy)**: Todas las perspectivas comparten la misma ventana definida en `configuracion_global`. Las perspectivas que no definan `ventana:` en su bloque heredan automáticamente la global. Este es el modo por defecto y mantiene compatibilidad total con configuraciones antiguas.

2. **Multi-ventana**: Cada perspectiva define su propia ventana en su bloque de configuración. Requisitos:
   - Todas las ventanas deben ser de tipo `temporal`
   - Solo se permiten unidades de duración **constante**: días, horas, minutos, segundos, semanas
   - ❌ No se admiten unidades de duración variable: meses, años

**Planificador "fin mínimo":** En modo multi-ventana, el sistema sincroniza automáticamente las perspectivas mediante la regla del fin mínimo: en cada iteración avanzan únicamente las perspectivas cuya ventana termina primero. Cuando múltiples perspectivas coinciden en el mismo `fin`, todas avanzan en el mismo tick. Esta estrategia mantiene las perspectivas coordinadas sin que ninguna avance más allá de las otras innecesariamente.

Ejemplo visual (control_flow: 1 día, calendar: 90 días):
```
Tick 1:  cf=[0d,1d]    cal=[0d,90d]    → Ambas disparan (inicial)
Tick 2:  cf=[12h,1d12h] cal=[0d,90d]   → Solo cf avanza (su fin < calendar)
...
Tick 90: cf=[89d,90d]  cal=[0d,90d]    → Ambas avanzan (fin coincide)
Tick 91: cf=[90d,91d]  cal=[30d,120d]  → Solo cal avanza
```

---

#### Auto-ajuste del tamaño de ventana

Cualquier ventana puede activar el **auto-ajuste automático** de `tamano_ventana`. El sistema busca el menor tamaño `n` tal que tres sub-ventanas consecutivas no solapadas de tamaño `n` produzcan comportamientos distintos para esa perspectiva. Si las tres son equivalentes, el proceso es estable en ese tramo y `n` se incrementa hasta encontrar un valor que sí capture cambio.

```
Desde posición P (inicio del log o punto del drift):
  n = n_min
  sub1=[P, P+n]  sub2=[P+n, P+2n]  sub3=[P+2n, P+3n]
  ¿son las tres equivalentes?
    Sí → proceso estable → n += delta → repetir
    No → n es el nuevo tamano_ventana
```

El mismo algoritmo se aplica automáticamente en **dos momentos** del ciclo de vida de la ejecución:

1. **Antes del primer tick (ajuste inicial):** el sistema calibra `tamano_ventana` para cada perspectiva con `autoajuste: true` partiendo del inicio del log (o del `fecha_inicial`/`primer_evento`/`primera_traza` declarado en el YAML, si lo hay). El objetivo es entrar al primer tick con una ventana ya adaptada a la dinámica del log, sin necesidad de esperar a que el detector confirme un primer drift. **En este momento también se calcula `salto_ventana` a partir del log** (ver subsección siguiente).

2. **Tras una confirmación de drift (ajuste post-drift):** cada vez que el detector confirma un drift en una perspectiva con `autoajuste: true`, `tamano_ventana` se recalibra partiendo de la traza donde se confirmó el drift, ajustándose a la dinámica del nuevo régimen del proceso. **`salto_ventana` NO se modifica** en este momento: conserva el valor fijado por el ajuste inicial.

En ambos momentos, los parámetros del detector `n_regresion` / `n_confirmacion` se recalibran automáticamente en función de `tamano_ventana / salto_ventana` (ver subsección siguiente).

**Equivalencia por perspectiva (sin re-entrenar modelos costosos):**

| Perspectiva (`modelo`) | Firma usada | Comparador |
|---|---|---|
| `inductive_miner`, `heuristic_miner` | Conjunto de pares directly-follows (DFG) | Igualdad exacta |
| `modelo_arrival_rate`, `modelo_service_rate` | Conteos del evento-clave por bucket temporal | Test KS pareado 3×3 |
| `modelo_calendarios` | Distribución (recurso, día\_semana, hora) normalizada | Distancia TV < umbral |
| `modelo_resource_productivity` | Distribución de eventos por recurso normalizada | Distancia TV < umbral |

**Configuración — los parámetros van dentro del bloque `ventana:` de cada perspectiva:**

```yaml
ventana:
  tipo: 'temporal'
  tamano_ventana: "1 days"
  salto_ventana: "12 hours"
  fecha_inicial: null
  # Auto-ajuste
  autoajuste: true               # Activar/desactivar (default: false)
  ajuste_n_min: "12 hours"      # Tamaño mínimo de búsqueda (default: 1% del span del log)
  ajuste_delta: "1 hour"        # Incremento por iteración  (default: 0.1% del span del log)
  ajuste_n_max: "30 days"       # Tope máximo              (default: 50% del span del log)
  ajuste_umbral: 0.05           # Umbral de equivalencia   (default: por perspectiva)
```

| Parámetro | Tipo | Default | Descripción |
|---|---|---|---|
| `autoajuste` | `boolean` | `false` | Activa la calibración inicial y el recálculo de `tamano_ventana` tras cada drift confirmado |
| `ajuste_n_min` | `string` \| `integer` | 1% del log | Tamaño mínimo a probar. `temporal`: string `pandas.Timedelta`. `eventos`/`trazas`: entero |
| `ajuste_delta` | `string` \| `integer` | 0.1% del log | Incremento de `n` por iteración del algoritmo |
| `ajuste_n_max` | `string` \| `integer` | 50% del log | Límite superior; se devuelve si se alcanza sin encontrar diferencias |
| `ajuste_umbral` | `float` | por perspectiva | KS: nivel alpha (default 0.05). TV: distancia máxima (default 0.10) |

> ⚠️ En **modo multi-ventana** el ajuste es independiente por perspectiva: cada una tiene su propio bloque de parámetros, así que no hay conflicto.
>
> 🚫 En **modo uni-ventana** con **más de una perspectiva**, el sistema **desactiva automáticamente `autoajuste`** al arrancar y emite un warning. La razón: todas las perspectivas comparten el mismo bloque de ventana, así que cualquier recalibrado entraría en conflicto con las demás (cada perspectiva intentaría ajustar el mismo `tamano_ventana` según su propio criterio de equivalencia). El YAML no se modifica; la desactivación afecta solo a la ejecución en curso. Si quieres usar `autoajuste` con varias perspectivas, migra a multi-ventana definiendo `ventana:` en cada perspectiva.
>
> ✅ En **modo uni-ventana con una sola perspectiva**, `autoajuste` funciona con normalidad — no hay conflicto posible porque solo hay un consumidor del bloque de ventana.
>
> 📌 Con `autoajuste: true`, el `salto_ventana` del YAML se ignora: el sistema lo calcula automáticamente durante el ajuste inicial (ver subsección siguiente) y lo mantiene fijo durante toda la ejecución. Si `autoajuste: false`, se respeta el valor declarado en el YAML.

---

#### Cálculo automático de `salto_ventana`

Cuando `autoajuste: true`, el sistema calcula `salto_ventana` durante el ajuste inicial como el desplazamiento que equivale, en promedio, a la incorporación de **una nueva traza** a la ventana del orquestador. Esta elección homogeneiza el significado de `n_confirmacion` entre los tres tipos de ventana ("cuántas trazas hacen falta para confirmar drift") y libera al usuario de tener que afinar a mano un valor distinto para cada tipo de log.

**Fórmulas por tipo de ventana:**

| Tipo de ventana | `salto_ventana` calculado |
|---|---|
| `trazas` | `1` (cada iteración avanza exactamente una traza, por definición) |
| `eventos` | `round(len(log) / nº trazas)`, clampado a un mínimo de 1 |
| `temporal` | mediana del inter-arrival time entre los primeros eventos de trazas consecutivas |

**Por qué cada elección:**

- **`eventos`**: una división simple sobre el log completo. Asume densidad de eventos razonablemente uniforme. Para logs muy heterogéneos puede ser un estimador sesgado, pero como punto de partida es predecible y rápido (una sola pasada por el log).
- **`temporal`**: se usa la **mediana** y no la media porque los inter-arrivals suelen tener outliers fuertes en logs reales (huecos largos sin trazas que distorsionan la media). La mediana es robusta a esos huecos.
- **`trazas`**: trivial; el tipo ya usa "trazas" como unidad nativa.

**Cuándo se calcula:**

- **Solo durante el ajuste inicial**, antes del primer tick. El valor queda fijado para toda la ejecución posterior.
- **No se recalcula en el ajuste post-drift**: aunque `tamano_ventana` pueda cambiar varias veces a lo largo de la ejecución, `salto_ventana` permanece como el calculado al principio. Esto asegura que `n_confirmacion = tamano_ventana / salto_ventana` siga representando consistentemente "trazas equivalentes hasta confirmar drift" sin saltos de escala entre fases.
- Si el cálculo no es posible (log vacío, una sola traza en `temporal`, tipo desconocido), se conserva el `salto_ventana` declarado en el YAML y se emite un log de aviso.

---

#### Recalibrado automático de `n_regresion` y `n_confirmacion`

Cada vez que el auto-ajuste modifica `tamano_ventana` (en el ajuste inicial o en el ajuste post-drift), el sistema **también recalibra automáticamente** los parámetros del detector `n_regresion` y `n_confirmacion` de esa perspectiva. Esto garantiza que el detector trabaje con un historial coherente con la nueva ventana del log: si el tamaño físico de la ventana cambia, el número de puntos del historial que el detector necesita observar para evaluar la regresión y confirmar el drift también debe cambiar.

**Fórmula:**

```
n_confirmacion = tamano_ventana / salto_ventana   (en iteraciones)
n_regresion    = n_confirmacion // 2
```

En este orquestador el detector recibe un punto del historial por cada iteración. Por tanto, una "ventana completa" del log equivale a `tamano_ventana / salto_ventana` iteraciones, que es el valor con el que se actualiza `n_confirmacion`. La ventana de regresión cubre la mitad inicial de la ventana de confirmación.

**Casos de uso:**

| Tipo de ventana | Tamaño / Salto | n_confirmacion | n_regresion |
|---|---|---|---|
| `temporal` | `90 days` / `1 day` | 90 | 45 |
| `temporal` | `1 days` / `12 hours` | 2 | 1 |
| `eventos` | `1800` / `18` | 100 | 50 |
| `trazas` | `100` / `1` | 100 | 50 |

Para ventanas temporales el cociente se calcula como ratio de `pandas.Timedelta`; para ventanas por eventos o trazas se hace división convencional. El resultado se clampa a un mínimo de 1 para evitar valores degenerados (por ejemplo, si `salto > tamano`).

**Cuándo se aplica:**

- Solo si `autoajuste: true` está activo en el bloque `ventana` de la perspectiva
- Se ejecuta inmediatamente después de actualizar `tamano_ventana` y `salto_ventana`, en cualquiera de los dos momentos:
  - **Ajuste inicial**, antes del primer tick: los nuevos valores rigen desde la primera llamada al detector
  - **Ajuste post-drift**: los nuevos valores entran en vigor en la siguiente llamada al detector

Tras un drift confirmado, el orquestador limpia los historiales `hist_candidatos` y `hist_valores` de la perspectiva, por lo que los nuevos `n_regresion` y `n_confirmacion` se aplican sobre un detector con historial vacío, sin contaminación de la fase previa.

**Logging:** el auto-ajuste emite líneas de aviso con un prefijo que identifica el momento del disparo (`ajuste_ventana_inicial` o `ajuste_ventana`), para facilitar el diagnóstico cuando ambos modos coexisten en una misma ejecución. El formato del log también refleja si `salto_ventana` cambió (flecha en el ajuste inicial) o si quedó fijo (sin flecha en el ajuste post-drift).

Ejemplo del ajuste inicial — `salto_ventana` se calcula a partir del log y cambia respecto al valor declarado en el YAML:

```
WARNING [ajuste_ventana_inicial] 'control_flow': tamano_ventana 1 days -> 2 days, salto_ventana 12 hours -> 1 day (calibración previa al primer tick desde posicion_inicial=2024-01-01 00:00:00).
WARNING [ajuste_ventana_inicial] 'control_flow': n_confirmacion 50 -> 2, n_regresion 25 -> 1.
```

Ejemplo del ajuste post-drift — `salto_ventana` se conserva fijo desde el ajuste inicial:

```
WARNING [ajuste_ventana] 'control_flow': tamano_ventana 2 days -> 4 days, salto_ventana=1 day (fijo desde el ajuste inicial) (drift en traza 1453).
WARNING [ajuste_ventana] 'control_flow': n_confirmacion 2 -> 4, n_regresion 1 -> 2.
```

> 💡 Si por algún motivo el cociente no se puede calcular (parámetros faltantes, división por cero, tipos incompatibles), el recalibrado se omite con un log informativo y los valores anteriores de `n_regresion` / `n_confirmacion` se conservan.

---

#### Modo `temporal`

Extrae eventos cuyo timestamp cae dentro de `[fecha_inicio, fecha_inicio + tamano_ventana]`. En cada iteración la fecha de inicio avanza `salto_ventana`.

**Uni-ventana (todas las perspectivas heredan):**

```yaml
configuracion_global:
  ruta_log: "./data/01_raw/cb-2500.xes"
  debug: true
  max_iter: null

  ventana:
    tipo:          'temporal'
    tamano_ventana: "100 days"    # pandas.Timedelta: "N days/hours/minutes"
    salto_ventana:  "1 day"
    fecha_inicial:  null          # null = fecha mínima del log
  
  primera_tarea: 'Loan_application_received'
  ultima_tarea:  'Finish_process'

perspectivas:
  - nombre: "control_flow"
    # No define ventana → hereda la global (100 días)
    op_filtrado: ["filtrado_control_flow"]
    modelo: "inductive_miner"
```

**Multi-ventana (cada perspectiva define la suya):**

```yaml
configuracion_global:
  ruta_log: "./data/01_raw/cb-2500.xes"
  debug: true
  
  primera_tarea: 'Loan_application_received'
  ultima_tarea:  'Finish_process'
  # No hay ventana global; cada perspectiva define la suya

perspectivas:
  - nombre: "control_flow"
    ventana:                       # ← Ventana propia: 1 día
      tipo: 'temporal'
      tamano_ventana: "1 days"
      salto_ventana: "12 hours"
      fecha_inicial: null
    op_filtrado: ["filtrado_control_flow"]
    modelo: "inductive_miner"

  - nombre: "calendar"
    ventana:                       # ← Ventana propia: 90 días
      tipo: 'temporal'
      tamano_ventana: "90 days"
      salto_ventana: "30 days"
      fecha_inicial: null
    op_filtrado: ["filtrado_calendarios"]
    modelo: "modelo_calendarios"
```

En este ejemplo, control_flow avanzará cada tick mientras calendar avanzará cada 7-8 ticks, sincronizándose automáticamente cuando sus ventanas alcancen los mismos `fin`.

---

#### Modo `eventos`

Extrae exactamente `tamano_ventana` eventos consecutivos del log ordenado por timestamp, desplazando `salto_ventana` eventos en cada iteración.

```yaml
configuracion_global:
  ruta_log: "./data/01_raw/cb-2500.xes"
  debug: true

  ventana:
    tipo:           'eventos'
    tamano_ventana: 1300     # Número de eventos por ventana (entero)
    salto_ventana:  13       # Eventos que avanza la ventana por iteración
    primer_evento:  0        # Índice (base 0) del primer evento inicial

  primera_tarea: 'Loan_application_received'
  ultima_tarea:  'Finish_process'
```

---

#### Modo `trazas`

Extrae exactamente `tamano_ventana` trazas completas, ordenadas por su primer evento en el tiempo. Es el modo recomendado para la perspectiva de control-flow, ya que garantiza que cada ventana contiene el mismo número de casos completos.

```yaml
configuracion_global:
  ruta_log: "./data/01_raw/cm-2500.xes"
  debug: true

  ventana:
    tipo:           'trazas'
    tamano_ventana: 100      # Número de trazas completas por ventana
    salto_ventana:  1        # Trazas que avanza la ventana por iteración
    primera_traza:  null     # null = desde la primera traza del log
```

---

### Referencia completa de parámetros

#### Sección `configuracion_global`

| Parámetro | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| `ruta_log` | `string` | ✅ | Ruta al fichero de log. Acepta `.xes` (control-flow) y `.csv` (arrival/service rate) |
| `debug` | `boolean` | ❌ | Si es `true`, exporta CSVs de diagnóstico por ventana y activa logs verbosos |
| `max_iter` | `integer` \| `null` | ❌ | Límite máximo de iteraciones. `null` itera hasta el fin del log |
| `primera_tarea` | `string` | ❌* | Actividad de inicio del proceso. *Obligatorio para la perspectiva `control_flow` (usa `filtrar_trazas_completas`), para `arrival_rate` (su filtrado retiene los eventos de inicio de traza) y siempre que cualquier perspectiva use `avance: "on_trace"` |
| `ultima_tarea` | `string` | ❌* | Actividad de fin del proceso. *Obligatorio para la perspectiva `control_flow` (usa `filtrar_trazas_completas`), para `service_rate` (su filtrado retiene los eventos de fin de traza) y siempre que cualquier perspectiva use `avance: "on_trace"` |

#### Bloque `ventana` — parámetros por modo

| Parámetro | Modo | Tipo | Obligatorio | Descripción |
|---|---|---|---|---|
| `tipo` | todos | `string` | ✅ | Selector del modo: `"temporal"`, `"eventos"` o `"trazas"` |
| `tamano_ventana` | `temporal` | `string` | ✅ | Duración de la ventana. Compatible con `pandas.Timedelta` (ej: `"5 days"`, `"2 hours"`, `"30 minutes"`). No se deben usar meses |
| `salto_ventana` | `temporal` | `string` | ✅ | Desplazamiento entre iteraciones. Mismas unidades que `tamano_ventana` |
| `fecha_inicial` | `temporal` | `string` \| `null` | ❌ | Inicio del análisis en formato `"YYYY-MM-DD HH:MM:SS"`. Si es `null`, se usa la fecha mínima del log. **En modo multi-ventana debe ser `null`** (todas las perspectivas arrancan en el inicio del log) |
| `tamano_ventana` | `eventos` | `integer` | ✅ | Número de eventos que componen cada ventana |
| `salto_ventana` | `eventos` | `integer` | ✅ | Número de eventos que avanza la ventana por iteración |
| `primer_evento` | `eventos` | `integer` \| `null` | ❌ | Índice (base 0) del primer evento de la ventana inicial. `null` = primer evento del log |
| `tamano_ventana` | `trazas` | `integer` | ✅ | Número de trazas completas por ventana |
| `salto_ventana` | `trazas` | `integer` | ✅ | Número de trazas que avanza la ventana por iteración |
| `primera_traza` | `trazas` | `integer` \| `null` | ❌ | Índice de inicio en la lista de trazas ordenadas. `null` = primera traza del log |

---

#### Sección `perspectivas[]` — parámetros comunes

| Parámetro | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| `nombre` | `string` | ✅ | Identificador único. Se usa en logs, nombres de fichero y resultados |
| `op_filtrado` | `list[string]` | ✅ | Operaciones de filtrado a aplicar **secuencialmente** sobre la ventana (puede ser `[]`) |
| `op_transformaciones` | `list[string]` | ✅ | Transformaciones a aplicar **secuencialmente** antes del descubrimiento del modelo (puede ser `[]`) |
| `modelo` | `string` | ✅ | Algoritmo único de descubrimiento o construcción del modelo. **No se admite lista** — una perspectiva entrena un solo modelo por iteración |
| `metricas` | `list[string]` | ✅ | Métricas a calcular en cada iteración. Se evalúan **en paralelo** sobre el mismo modelo; cada una mantiene su propia racha en el detector (puede ser `[]`) |
| `op_det_concept_drift` | `string` \| `null` | ❌ | Algoritmo único de detección. **No se admite lista** — todas las métricas de la perspectiva pasan por el mismo detector. `null` desactiva la detección |
| `n_regresion` | `integer` | ❌* | Tamaño de la ventana de regresión lineal usada por los algoritmos de detección. *Obligatorio si `op_det_concept_drift` no es `null` |
| `n_confirmacion` | `integer` | ❌* | Número de ventanas consecutivas con degradación para confirmar un drift. *Obligatorio si `op_det_concept_drift` no es `null` |
| `avance` | `string` | ❌ | Política de ejecución por iteración: `"on_trace"` o `"nada"` (ver tabla siguiente) |

> 💡 **Encadenables vs únicos.** En cada perspectiva, `op_filtrado`, `op_transformaciones` y `metricas` son **listas** y se pueden encadenar (o dejar vacías). En cambio, `modelo` y `op_det_concept_drift` son **strings**: una perspectiva entrena un único modelo por iteración y lo evalúa con un único detector — todas sus métricas pasan por ese mismo detector con racha independiente por métrica.

#### Algoritmos de detección (`op_det_concept_drift`)

| Valor | Aplicable a | Descripción |
|---|---|---|
| `"deteccion_regresion"` | cualquier métrica escalar (fitness, precision, MAE, MSE, soporte, ...) | Regresión lineal sobre los últimos `n_regresion` valores. Marca la ventana como candidata si la pendiente es significativa (p < 0.05); confirma drift cuando `n_confirmacion` candidatas consecutivas mantienen el mismo signo. La traza inicial de la racha se registra como inicio del drift. Inspirado en el algoritmo C2D2 de Gallego-Fontenla et al. (2023). |
| `"deteccion_distribucion"` | métrica `comparar_pertenencia_a_distribucion` (por par recurso-tarea) | La métrica produce un bool por par `(recurso, tarea)` indicando si la muestra de la ventana sigue perteneciendo a la distribución de referencia. Esta detección lleva una racha independiente por par; confirma drift cuando un par acumula `n_confirmacion` ventanas "fuera de distribución" consecutivas. |
| `null` | todas | Desactiva la detección. El modelo se entrena y las métricas se calculan, pero no se emite ninguna alerta de drift |

#### Política de avance (`avance`)

| Valor | Descripción |
|---|---|
| `"on_trace"` | La perspectiva **solo se ejecuta** cuando entra una traza completa nueva en la ventana (verificado comparando con la ventana anterior mediante `filtrar_trazas_completas`). Evita cómputos redundantes en logs densos. Requiere `primera_tarea` y `ultima_tarea` en `configuracion_global`. |
| `"nada"` | La perspectiva **siempre se ejecuta** en cada iteración, independientemente de si entró una nueva traza. Comportamiento estándar para perspectivas ML que trabajan con agregaciones temporales. |

---

#### Parámetros específicos por perspectiva

**Perspectiva `arrival_rate`:**

| Parámetro | Tipo | Obligatorio | Ejemplo | Descripción |
|---|---|---|---|---|
| `frecuencia_arrival_rate` | `string` | ✅ | `"15 min"` | Frecuencia de muestreo interna: define el intervalo sobre el que se cuenta cada llegada |
| `granularidad_arrival_rate` | `string` | ✅ | `"1 hour"` | Tamaño de la sub-ventana de agregación de llegadas dentro de cada ventana principal |
| `metrica_validacion_modelo` | `string` | ✅ | `"MAE"` | Métrica usada en el tuneo de hiperparámetros para seleccionar el mejor modelo. Valores admitidos: `"MAE"` o `"MSE"` (se traducen internamente al scorer de sklearn correspondiente vía `METRICAS_VALIDACION_MODELO`) |

**Perspectiva `service_rate`:**

| Parámetro | Tipo | Obligatorio | Ejemplo | Descripción |
|---|---|---|---|---|
| `frecuencia_service_rate` | `string` | ✅ | `"15 min"` | Frecuencia de muestreo interna para el conteo de finalizaciones de traza |
| `granularidad_service_rate` | `string` | ✅ | `"1 hour"` | Tamaño de la sub-ventana de agregación de finalizaciones |
| `metrica_validacion` | `string` | ✅ | `"MAE"` | Métrica usada en el tuneo de hiperparámetros durante el entrenamiento. Valores admitidos: `"MAE"` o `"MSE"` (se traducen internamente al scorer de sklearn correspondiente vía `METRICAS_VALIDACION_MODELO`) |

---

## 🔭 Perspectivas Disponibles

El sistema implementa tres perspectivas de análisis, cada una con su propio pipeline de filtrado, transformación, modelado y detección.

---

### 1. Control Flow

Analiza la estructura y el orden de las actividades del proceso mediante Redes de Petri. Es la perspectiva principal para detectar cambios estructurales en el flujo de control.

**Métricas disponibles:**

| Clave | Nombre | Descripción técnica |
|---|---|---|
| `"fitness"` | Fitness | Porcentaje de trazas del log reproducibles (*replayed*) en el modelo. Calculado con `pm4py.fitness_token_based_replay`. Detecta nuevo comportamiento no contemplado en el modelo. |
| `"precision"` | Precision (PC) | Métrica PC: `1 - |OLP \ DFR| / |OLP|`. **OLP** = pares de actividades directamente conectadas en el modelo (calculado vía `pm4py.play_out`). **DFR** = pares directamente sucesivos observados en el log. Detecta comportamiento del modelo que desaparece del log. Inspirada en el algoritmo C2D2 de Gallego-Fontenla et al. (2023). |

#### Columnas necesarias

El log debe estar en **formato XES estándar**, legible con `pm4py`. Las columnas mínimas requeridas son:

| Columna | Descripción | Obligatoria |
|---|---|---|
| `case:concept:name` | Identificador único del caso/traza | ✅ |
| `concept:name` | Nombre de la actividad ejecutada en el evento | ✅ |
| `time:timestamp` | Marca temporal del evento | ✅ |

> Los algoritmos `filtrar_trazas_completas` (usado con `avance: "on_trace"`) requieren además que los valores de `primera_tarea` y `ultima_tarea` estén presentes en la columna `concept:name`.

**Fichero de referencia:** `conf/ventana_trazas.yml`

---

### 2. Arrival Rate

Modela la **tasa de llegada de casos** mediante un `RandomForestRegressor`. Detecta drift cuando la capacidad predictiva del modelo se degrada, indicando un cambio en el patrón de llegadas.

**Pipeline interno:**
1. `filtrado_arrival_rate` — extrae el primer evento de cada traza (inicio de caso)
2. `transformacion_arrival_rate` — agrega llegadas por sub-ventanas de `frecuencia_arrival_rate`, extrae componentes temporales (hora, día semana, fin de semana, etc.) y aplica **codificación cíclica** (sin/cos) para capturar periodicidades
3. Generación de **lag features** (valores de arrival rate retrasados N periodos)
4. Entrenamiento con `RandomForestRegressor` y `RandomizedSearchCV` + `TimeSeriesSplit`

**Métrica disponible:** `MAE`

#### Columnas necesarias

La función `filtrado_arrival_rate` retiene únicamente los eventos cuyo `concept:name` coincide con `primera_tarea`, por lo que esa actividad debe estar presente en el log. El log puede ser `.xes` o `.csv`.

| Columna | Descripción | Obligatoria |
|---|---|---|
| `case:concept:name` | Identificador único del caso/traza | ✅ |
| `concept:name` | Nombre de la actividad (debe contener el valor de `primera_tarea`) | ✅ |
| `time:timestamp` | Marca temporal del evento | ✅ |
| `trace_real_index` | Índice ordinal de la traza en el log global, gestionado internamente por el framework | ✅ |

**Fichero de referencia:** `conf/arrival_rate.yml`

---

### 3. Service Rate

Modela la **tasa de finalización de casos** mediante `RandomForestRegressor`. Detecta drift cuando el patrón de finalización de trazas cambia significativamente.

**Diferencias respecto a arrival rate:**
- Filtra el **último evento** de cada traza en lugar del primero
- Usa `GridSearchCV` (búsqueda exhaustiva) en lugar de `RandomizedSearchCV`
- Aplica `VarianceThreshold` para eliminar features de varianza cero antes del entrenamiento

**Métrica disponible:** `MAE`

#### Columnas necesarias

La función `filtrado_service_rate` retiene los eventos cuyo `concept:name` coincide con `ultima_tarea`. El log puede ser `.xes` o `.csv`.

| Columna | Descripción | Obligatoria |
|---|---|---|
| `case:concept:name` | Identificador único del caso/traza | ✅ |
| `concept:name` | Nombre de la actividad (debe contener el valor de `ultima_tarea`) | ✅ |
| `time:timestamp` | Marca temporal del evento | ✅ |
| `trace_real_index` | Índice ordinal de la traza en el log global, gestionado internamente por el framework | ✅ |

**Fichero de referencia:** `conf/service_rate.yml`

---

### 4. Resource Profiles

Analiza el comportamiento de los **recursos humanos** que participan en el proceso a través de cinco sub-perspectivas complementarias. Detecta drift cuando los patrones de colaboración, productividad, especialización, utilización o preferencias horarias de los recursos cambian a lo largo del tiempo. No requiere un modelo predictivo: las métricas se calculan directamente sobre cada ventana.

> 🔗 Las sub-perspectivas **`resource_productivity`** y **`resource_utilization`** son **consumidoras** de la perspectiva **`calendar`** (calendarios de recursos), ya que necesitan el calendario de cada recurso para calcular el tiempo efectivo de procesamiento. Si la perspectiva `calendar` no está declarada en la configuración, ambas funcionan de forma autónoma descubriendo el calendario por sí mismas. Ver la sección **[Dependencias entre perspectivas](#-dependencias-entre-perspectivas)** para más detalle.

**Pipeline interno:**
1. Selección del filtro adecuado según la sub-perspectiva (`filtrado_resource_colab`, `filtrado_resource_productivity`, etc.)
2. Aplicación opcional de transformaciones: `transformacion_resource_productivity` calcula el **TPA** (intersección del intervalo del evento con el calendario del recurso), mientras que `transformacion_resource_utilization` calcula el **TP** (processing time crudo, `time:timestamp - start_timestamp`)
3. Cálculo directo de la métrica sobre la ventana sin modelo ML

**Métricas disponibles:**

| Clave | Nombre técnico | Descripción |
|---|---|---|
| `"resource_colab"` | Colaboración entre recursos | Fracción media de casos en los que cada par de recursos $(r_1, r_2)$ trabajan juntos. |
| `"resource_skill"` | Especialización (instance count) | Número medio de instancias que cada par (recurso, tarea) ejecuta en la ventana. |
| `"resource_productivity"` | Productividad (PDR) | Cociente entre el TPA del recurso y el TPA medio global de la tarea: `TPA_recurso / TPA_medio_tarea`. |
| `"resource_utilization"` | Utilización | Fracción del tiempo disponible del recurso (según calendario) que dedica a procesar actividades: `ΣTP / T_A`, donde `T_A` es el tiempo laborable del recurso entre `τ_min` y `τ_max` de la ventana. |
| `"comparar_pertenencia_a_distribucion"` | Productividad — pertenencia a distribución | Para cada par `(recurso, tarea)` ajusta una distribución de referencia (familia paramétrica elegida por AIC) y, en cada ventana, devuelve un `bool` por par indicando si la muestra actual sigue perteneciendo a esa distribución. Es la métrica que consume `deteccion_distribucion`. |

#### Columnas necesarias

Los requisitos varían según el filtro utilizado. La tabla indica qué columnas exige cada función de filtrado:

| Columna | Descripción | Filtros que la requieren |
|---|---|---|
| `case:concept:name` | Identificador único del caso/traza | `filtrado_resource_colab`, `filtrado_resource_productivity` |
| `concept:name` | Nombre de la actividad ejecutada | `filtrado_resource_colab`, `filtrado_resource_productivity`, `filtrado_resource_skill` |
| `org:resource` | Identificador del recurso que ejecuta el evento | Todos |
| `start_timestamp` | Marca temporal de **inicio** de la actividad | `filtrado_resource_productivity`, `filtrado_resource_utilization` |
| `time:timestamp` | Marca temporal de **fin** de la actividad | `filtrado_resource_productivity`, `filtrado_resource_utilization` |

> ⚠️ Las columnas `start_timestamp` y `org:resource` no forman parte del estándar XES básico. Los logs `.xes` que incluyan los atributos `startTimestamp` y `org:resource` son compatibles; en caso contrario deben añadirse manualmente o generarse en un paso de preprocesado previo.

---

### 5. Calendarios de Recursos

Descubre los **calendarios de disponibilidad de recursos** mediante el algoritmo *fuzzy* (`discovery_fuzzy_resource_calendars_and_performances`) de [pix-framework](https://github.com/AutomatedProcessImprovement/pix-framework). Modela las franjas horarias y días de la semana en los que cada recurso suele estar activo, y detecta drift cuando ese patrón de disponibilidad cambia significativamente entre ventanas.

> 🔗 Esta perspectiva actúa como **productora** para `resource_productivity` y `resource_utilization`: cuando descubre o actualiza el calendario, las perspectivas consumidoras pasan a usar ese mismo modelo en lugar de descubrir uno propio. Ver **[Dependencias entre perspectivas](#-dependencias-entre-perspectivas)**.

**Pipeline interno:**
1. `filtrado_calendarios` — selecciona las columnas estándar del pix-framework y renombra según `EventLogIDs`
2. `transformacion_calendarios` — normaliza tipos (timestamps con zona horaria UTC, recurso como `str`), rellena recursos vacíos y ordena por `start_time`
3. `modelo_calendarios` — ejecuta `discovery_fuzzy_resource_calendars_and_performances` para obtener un calendario de intervalos por recurso

**Métricas disponibles:**

| Clave | Nombre | Descripción técnica |
|---|---|---|
| `"soporte"` | Soporte del calendario | Fracción media de eventos del log cubiertos por al menos un intervalo del calendario del recurso correspondiente. |
| `"soporte_invertido"` | Soporte invertido | Fracción media de intervalos del calendario que están respaldados por al menos un evento real del log. |

#### Columnas necesarias

La función `filtrado_calendarios` usa los identificadores estándar del pix-framework (`DEFAULT_XES_IDS`), que corresponden a las siguientes columnas del log:

| Columna en el log | Atributo `DEFAULT_XES_IDS` | Descripción | Obligatoria |
|---|---|---|---|
| `case:concept:name` | `case` | Identificador único del caso/traza | ✅ |
| `concept:name` | `activity` | Nombre de la actividad ejecutada | ✅ |
| `start_timestamp` | `start_time` | Marca temporal de **inicio** de la actividad | ✅ |
| `time:timestamp` | `end_time` | Marca temporal de **fin** de la actividad | ✅ |
| `time:enabled` | `enabled_time` | Marca temporal de **habilitación** de la actividad | ❌ (opcional) |
| `org:resource` | `resource` | Identificador del recurso que ejecuta el evento | ✅ |

> ⚠️ Al igual que en Resource Profiles, `start_timestamp` y `org:resource` deben estar presentes en el log. Si el fichero `.xes` no los incluye de forma nativa, es necesario un paso de conversión previo (véase `src/convertidor.py`).

---

### 🔗 Dependencias entre perspectivas

Algunas perspectivas necesitan **información producida por otras** para poder calcular sus métricas. Cuando se da esta relación, la perspectiva que consume la información se denomina **consumidora** y la que la produce, **productora**. El framework gestiona automáticamente esta relación, de modo que el usuario solo tiene que declarar las perspectivas que quiere ejecutar en su YAML de configuración.

#### Dependencias actuales

| Consumidora | Productora | Información compartida |
|---|---|---|
| `resource_productivity` | `calendar` | Calendario de disponibilidad de cada recurso |
| `resource_utilization` | `calendar` | Calendario de disponibilidad de cada recurso |

#### Cómo funciona

- **Si la productora está declarada en el YAML**: el framework se asegura de que la consumidora **no se ejecute** en una iteración hasta que la productora haya descubierto su primer modelo. A partir de ahí, la consumidora recibe automáticamente ese modelo y lo utiliza en lugar de calcularlo por su cuenta.
- **Cuando la productora detecta un drift y descubre un modelo nuevo**, la consumidora pasará a usar ese modelo actualizado en las iteraciones siguientes (no inmediatamente: solo cuando su propia ventana sea compatible con el periodo en el que se descubrió el modelo nuevo). En ese momento, el historial de métricas de la consumidora se reinicia, porque los valores calculados con el modelo viejo no son comparables con los del nuevo.
- **Si la productora NO está declarada en el YAML**: la consumidora funciona de forma **completamente autónoma**, descubriendo por sí misma la información que necesita en cada iteración. Para usar este modo basta con no incluir la perspectiva productora en la lista `perspectivas` del fichero de configuración.

#### Coherencia de tamaños de ventana en multi-ventana

En modo **multi-ventana** (donde productora y consumidora pueden declarar tamaños de ventana distintos), el framework verifica automáticamente antes del primer tick que los tamaños sean coherentes:

> Cuando una perspectiva consumidora declara una ventana **mayor** que la de su productora, se rompe la semántica de la dependencia: una ventana del consumidor abarcaría eventos pertenecientes a varios modelos consecutivos de la productora, pero el mecanismo de propagación de modelos solo le entrega uno (el activo al inicio de su ventana), así que los eventos del final de la ventana del consumidor se evaluarían con un modelo ya obsoleto.

Para evitarlo, el framework **agranda automáticamente la ventana de la productora** hasta igualar la de la consumidora más grande que dependa de ella, y emite un warning informando del ajuste. Se opta por agrandar la productora (en vez de reducir la consumidora) por dos motivos:

1. Una misma productora suele dar servicio a varias consumidoras, así que ajustarla una sola vez beneficia a todas.
2. Si el usuario eligió un tamaño concreto para su consumidora, hay que respetar esa intención.

Tras agrandar la productora, el framework recalcula automáticamente su `salto_ventana` y los parámetros del detector (`n_confirmacion`, `n_regresion`), para que sigan siendo coherentes con la nueva ventana. El caso opuesto (productora mayor que consumidora) **no se considera problemático** y no se interviene: cada ventana de la consumidora cae naturalmente dentro de un único modelo de la productora, que es justo el comportamiento deseado.

---

### Registro completo de operaciones

| Registro | Clave YAML | Estado | Descripción |
|---|---|---|---|
| **Filtrado** | `filtrar_trazas_completas` | ✅ | Retiene solo trazas que contienen `primera_tarea` y `ultima_tarea`. Obligatorio con `avance: "on_trace"`. |
| | `filtrado_arrival_rate` | ✅ | Extrae el primer evento de cada traza para calcular la tasa de llegada. |
| | `filtrado_service_rate` | ✅ | Extrae el último evento de cada traza para calcular la tasa de servicio. |
| | `filtrado_calendarios` | ✅ | Selecciona las columnas estándar de pix-framework y las renombra según `EventLogIDs`. |
| | `filtrado_resource_colab` | ✅ | Retiene `case:concept:name`, `concept:name` y `org:resource` para el cálculo de colaboración. |
| | `filtrado_resource_productivity` | ✅ | Retiene `case:concept:name`, `concept:name`, `start_timestamp`, `time:timestamp` y `org:resource`. |
| | `filtrado_resource_skill` | ✅ | Retiene `concept:name` y `org:resource` para el cálculo de especialización. |
| | `filtrado_resource_utilization` | ✅ | Retiene `org:resource`, `start_timestamp` y `time:timestamp`. |
| **Transformación** | `transformacion_simple` | ✅ | Pasa el log sin modificaciones. |
| | `transformacion_arrival_rate` | ✅ | Agrega llegadas, extrae features temporales y calcula la tasa de llegada por sub-ventana. |
| | `transformacion_service_rate` | ✅ | Agrega finalizaciones, extrae features temporales y calcula la tasa de servicio por sub-ventana. |
| | `transformacion_calendarios` | ✅ | Normaliza tipos, rellena recursos vacíos y ordena por `start_time` para pix-framework. |
| | `transformacion_resource_productivity` | ✅ | Calcula el TPA (intersección de `time:timestamp - start_timestamp` con el calendario del recurso) en segundos por evento. |
| | `transformacion_resource_utilization` | ✅ | Calcula el TP (`time:timestamp - start_timestamp`) en segundos por evento. La métrica obtiene el calendario en cada ventana para calcular `T_A`. |
| **Modelos** | `inductive_miner` | ✅ | Red de Petri con Inductive Miner. Calcula OLP automáticamente con `pm4py.play_out`. |
| | `heuristic_miner` | ✅ | Red de Petri con Heuristic Miner. También calcula OLP. |
| | `modelo_arrival_rate` | ✅ | `RandomForestRegressor` con codificación cíclica, lag features y `RandomizedSearchCV`. |
| | `modelo_service_rate` | ✅ | `RandomForestRegressor` con `GridSearchCV`. |
| | `modelo_calendarios` | ✅ | Descubrimiento de calendarios fuzzy por recurso con `pix-framework`. |
| | `distribuciones_temporales` | ✅ | Ajusta una distribución de referencia (AIC sobre familias candidatas configurables) por par `(recurso, tarea)` a partir del TPA. Implementado en `modelo_resource_productivity`. |
| **Métricas** | `fitness` | ✅ | Porcentaje de trazas reproducibles (token-based replay). |
| | `precision` | ✅ | Métrica PC: `1 - \|OLP \ DFR\| / \|OLP\|`. |
| | `MAE` | ✅ | Mean Absolute Error del modelo ML sobre la ventana actual. |
| | `MSE` | ✅ | Mean Squared Error del modelo ML sobre la ventana actual. |
| | `soporte` | ✅ | Fracción media de eventos del log cubiertos por el calendario del recurso. |
| | `soporte_invertido` | ✅ | Fracción media de intervalos del calendario respaldados por eventos reales. |
| | `resource_colab` | ✅ | Fracción media de casos donde cada par de recursos colabora. |
| | `resource_skill` | ✅ | Número medio de instancias por par (recurso, tarea). |
| | `resource_utilization` | ✅ | Cociente `ΣTP / T_A` por recurso, donde `T_A` es el tiempo laborable del recurso en la ventana según su calendario. |
| | `comparar_pertenencia_a_distribucion` | ✅ | Para cada par `(recurso, tarea)`, devuelve un `bool` indicando si la muestra de la ventana sigue perteneciendo a la distribución de referencia ajustada con `distribuciones_temporales`. Consumida por `deteccion_distribucion`. |
| **Detección** | `deteccion_regresion` | ✅ | Regresión lineal sobre los últimos `n_regresion` valores de cualquier métrica escalar; confirma drift cuando `n_confirmacion` ventanas consecutivas mantienen pendiente significativa del mismo signo. Inspirado en C2D2 (Gallego-Fontenla et al. 2023). |
| | `deteccion_distribucion` | ✅ | Racha de pertenencia a distribución por par `(recurso, tarea)`. Confirma drift cuando un par acumula `n_confirmacion` ventanas "fuera de distribución" consecutivas. |

> 💡 **Extensibilidad**: para añadir una nueva operación, impleméntala en el módulo correspondiente de `src/perspectivas/` y regístrala en el diccionario `REGISTRO_*` de `src/registro_op.py`.

---

## 💻 Uso

### Ejecución estándar (uni-ventana legacy)

```bash
python -m src.main_flow -f conf/parameters.yml
```

Todas las perspectivas comparten la misma ventana (100 días con salto de 1 día).

### Ejecución multi-ventana (ventanas independientes)

```bash
python -m src.main_flow -f conf/parameters_multi_ventana.yml
```

Ejecuta control flow con ventana de 1 día y calendar con ventana de 90 días, sincronizadas automáticamente mediante el planificador "fin mínimo". Especialmente útil para perspectivas con horizontes temporales muy distintos.

### Perspectivas individuales

**Control-flow con ventana por trazas:**

```bash
python -m src.main_flow -f conf/ventana_trazas.yml
```

**Control-flow con ventana por eventos:**

```bash
python -m src.main_flow -f conf/ventana_eventos.yml
```

**Perspectiva arrival rate (1 día):**

```bash
python -m src.main_flow -f conf/arrival_rate.yml
```

**Perspectiva service rate (5 días):**

```bash
python -m src.main_flow -f conf/service_rate.yml
```

**Perspectiva calendar (7 días):**

```bash
python -m src.main_flow -f conf/calendar.yml
```

### Modo mono-perspectiva

Ejecuta únicamente la primera perspectiva definida en el YAML:

```bash
python -m src.main_flow -f conf/parameters.yml --mono_perspectiva
```

### Limitar iteraciones (útil para pruebas)

```yaml
# En el YAML de configuración:
configuracion_global:
  max_iter: 5
```

### Salida esperada

```
INFO  - Log cargado: ./data/01_raw/cm-2500.xes

INFO  - INICIANDO ITERACIÓN 1
INFO  - Iniciando filtrado con una ventana de 100 trazas
INFO  - Descubriendo Red de Petri usando Inductive Miner
INFO  - Guardando el modelo descubierto como ./data/06_models/PetriNet_InductiveMiner_20240515_103012_0.png
INFO  - OLP calculado: {('A', 'B'), ('B', 'C'), ('C', 'D'), ...} | Longitud OLP: 12
INFO  - Calculando fitness (Percentage of fitting traces): 1.00
INFO  - Calculando precision: 0.92

INFO  - INICIANDO ITERACIÓN 2
...

WARNING - Concept drift detectado en la iteración 47 para la perspectiva control_flow.
INFO    - Registro del cambio: {'cambio_detectado': True, 'iteracion': 47, 'trace_real_index': 146}
INFO    - Se ha detectado un cambio. Redescubriendo modelo en la siguiente iteración.
```

### Validación de configuración

Al iniciar la ejecución, el sistema **valida automáticamente** la configuración de ventanas y determina el modo apropiado (`uni` o `multi`).

**Validaciones realizadas:**

1. En modo multi-ventana: todas las ventanas propias deben ser de tipo `temporal`
2. En **cualquier ventana temporal** (uni o multi): solo se admiten unidades de duración constante
   - ✅ Válidas: `"1 days"`, `"12 hours"`, `"30 minutes"`, `"2 weeks"`
   - ❌ Inválidas: `"3 months"`, `"1 year"` (duración variable)
3. En modo multi-ventana: ninguna perspectiva puede declarar `fecha_inicial` no-null — todas arrancan en el inicio natural del log para preservar una base temporal común
4. En modo uni-ventana: si hay ventanas no temporales, todas deben ser idénticas

**Errores de validación:**

```
ValueError: Multi-ventana solo admite ventanas de tipo 'temporal'. 
            Tipos detectados: ['eventos', 'temporal'].

ValueError: Perspectiva 'calendar': 'tamano_ventana'='3 months' usa la 
            unidad 'months', no admitida en ventanas temporales porque su 
            duración no es constante. Use días, horas, minutos o segundos.

ValueError: Perspectiva 'control_flow': en modo multi-ventana no 
            se admite 'fecha_inicial' declarada ('2020-01-01'); todas las 
            perspectivas deben arrancar en el inicio natural del log. 
            Deja 'fecha_inicial: null' u omítelo.
```

Si la validación falla, la ejecución se detiene y se muestra el error detallado. Revisa la configuración YAML y asegúrate de que cumple los requisitos del modo que intentas usar.

---

import itertools
from datetime import time

import scipy.stats as st
import pandas as pd
import numpy as np
import logging
from typing import Optional, Tuple, Dict, Any
from ..logging_config import get_logger
from pix_framework.io.event_log import DEFAULT_XES_IDS, EventLogIDs
from pix_framework.discovery.resource_calendar_and_performance.fuzzy.discovery import discovery_fuzzy_resource_calendars_and_performances
from .calendar import transformacion_calendarios


# Mapeo del índice de día de la semana (datetime.weekday()) a la cadena usada por pix-framework.
WEEKDAYS = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']

# Mapeo entre nomenclatura XES (con ':' en las columnas) e identificadores que espera
# pix-framework. pix-framework usa itertuples() internamente y los ':' de XES no son
# atributos válidos, por lo que renombramos *solo* en los puntos de contacto con
# pix-framework. El resto del pipeline opera con nomenclatura XES (DEFAULT_XES_IDS).
_PIX_IDS = EventLogIDs()
_XES_A_PIX = {
    DEFAULT_XES_IDS.case: _PIX_IDS.case,
    DEFAULT_XES_IDS.activity: _PIX_IDS.activity,
    DEFAULT_XES_IDS.start_time: _PIX_IDS.start_time,
    DEFAULT_XES_IDS.end_time: _PIX_IDS.end_time,
    DEFAULT_XES_IDS.enabled_time: _PIX_IDS.enabled_time,
    DEFAULT_XES_IDS.resource: _PIX_IDS.resource,
}

logger = get_logger(__name__)

# Familias candidatas por defecto. El YAML puede sobreescribir
# esta lista vía el parámetro `familias_candidatas` de la perspectiva.
_FAMILIAS_DEFAULT = ['lognorm', 'expon', 'gamma', 'norm', 'uniform']
# Familias con soporte estrictamente positivo: en ellas el fit debe fijar floc=0 para
# evitar que el parámetro de localización absorba sesgos y la familia gane el AIC
# artificialmente (ver _encontrar_mejor_distribucion).
_FAMILIAS_SOPORTE_POSITIVO = {'lognorm', 'expon', 'gamma'}

# Percentiles para winsorizar las productividades antes de ajustar la distribución. 
# El valor por debajo del percentil inferior sube a ese percentil 
# y el de por encima del superior baja al suyo. Elimina outliers extremos.
_PERCENTILES_WINSOR = (5, 95)


def _winsorizar(datos: np.ndarray, percentiles: tuple = _PERCENTILES_WINSOR) -> np.ndarray:
    """
    Winsoriza un array: lleva los valores por debajo del percentil inferior y por
    encima del superior al valor de esos percentiles (cap), sin descartar muestras (a
    diferencia del trimming). Limita la influencia de los outliers en el ajuste posterior
    de la distribución conservando el tamaño de la muestra.
    """
    if len(datos) == 0:
        return datos
    p_inf, p_sup = np.percentile(datos, percentiles)
    return np.clip(datos, p_inf, p_sup)


def _resolver_familias_candidatas(nombres: Optional[list]) -> list:
    """
    Resuelve una lista de nombres de scipy.stats (strings) en los objetos de distribución
    correspondientes. Si `nombres` es None o vacío, se usa la lista por defecto
    `_FAMILIAS_DEFAULT`. Lanza ValueError si algún nombre no es una distribución continua
    válida (rv_continuous) en scipy.stats.
    """
    if not nombres:
        nombres = _FAMILIAS_DEFAULT

    resueltas = []
    for nombre in nombres:
        familia = getattr(st, nombre, None)
        if not isinstance(familia, st.rv_continuous):
            raise ValueError(
                f"Familia '{nombre}' no es una distribución continua válida en scipy.stats."
            )
        resueltas.append(familia)
    return resueltas


############################
# --- RESOURCE-PROFILE --- #
############################

# --- TAREAS DE FILTRADO ---
   
def filter_resource_colab(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Filtra el log para el cálculo de resource collaboration.
    """

    DEBUG = parametros.get('debug', False)

    logger.info("Filtrando log para cálculo de resource collaboration")

    # Se filtra el log para quedarnos solo con las columnas relevantes para colaboración: caso, actividad y recurso
    log_filtrado = log[[DEFAULT_XES_IDS.case, DEFAULT_XES_IDS.activity, DEFAULT_XES_IDS.resource]].copy()

    if DEBUG:
        log_filtrado.to_csv(f"./logs/resource_colab_filtrado.csv", index=False)

    return log_filtrado

def filter_resource_productivity(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Filtra el log para el cálculo de resource productivity, conservando los nombres XES.
    """

    DEBUG = parametros.get('debug', False)

    logger.info("Filtrando log para cálculo de resource productivity")

    # Seleccionar columnas necesarias en nomenclatura XES. El renombrado a identificadores
    # válidos se hace solo dentro de los puntos de contacto con pix-framework (ver
    # _preparar_log_calendarios y _extraer_calendario).
    columnas_necesarias = [DEFAULT_XES_IDS.case, DEFAULT_XES_IDS.activity, DEFAULT_XES_IDS.start_time, DEFAULT_XES_IDS.end_time, DEFAULT_XES_IDS.enabled_time, DEFAULT_XES_IDS.resource]

    log_filtrado = log[columnas_necesarias].copy()

    if DEBUG:
        log_filtrado.to_csv(f"./data/02_intermediate/resource_productivity_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_filtrado.csv", index=False)

    return log_filtrado

def filter_resource_skill(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Filtra el log para el cálculo de resource skill.
    """

    DEBUG = parametros.get('debug', False)

    logger.info("Filtrando log para cálculo de resource skill")

    # Se filtra el log para quedarnos solo con las columnas relevantes para skill: actividad y recurso
    log_filtrado = log[[DEFAULT_XES_IDS.activity, DEFAULT_XES_IDS.resource]].copy()

    if DEBUG:
        log_filtrado.to_csv(f"./logs/resource_skill_filtrado.csv", index=False)

    return log_filtrado

def filter_resource_utilization(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Filtra el log para el cálculo de resource utilization, conservando los nombres XES.
    """

    DEBUG = parametros.get('debug', False)

    logger.info("Filtrando log para cálculo de resource utilization")

    # Seleccionar columnas necesarias en nomenclatura XES. El renombrado a identificadores
    # válidos se hace solo dentro de los puntos de contacto con pix-framework (ver
    # _preparar_log_calendarios y _extraer_calendario).
    columnas_necesarias = [DEFAULT_XES_IDS.case, DEFAULT_XES_IDS.activity, DEFAULT_XES_IDS.start_time, DEFAULT_XES_IDS.end_time, DEFAULT_XES_IDS.enabled_time, DEFAULT_XES_IDS.resource]

    log_filtrado = log[columnas_necesarias].copy()

    if DEBUG:
        log_filtrado.to_csv(f"./data/02_intermediate/resource_utilization_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_filtrado.csv", index=False)

    return log_filtrado

# --- TAREAS DE TRANSFORMACIÓN ---

def _preparar_log_calendarios(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Adapta el log para que sea compatible con pix-framework, delegando en la transformación de la perspectiva
    de calendarios. Se utiliza como paso previo al descubrimiento del calendario de los recursos.

    Recibe y devuelve el log en nomenclatura XES. Internamente lo renombra a identificadores
    válidos porque `transformacion_calendarios` (perspectiva calendarios) opera con ese esquema,
    y vuelve a devolverlo en XES para que el resto del pipeline mantenga la convención.
    """

    parametros['debug'] = False  # para evitar que la transformación de calendarios genere logs intermedios

    log_transformado = transformacion_calendarios(log, parametros)

    parametros['debug'] = True

    return log_transformado

def _obtener_calendario(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Resuelve el calendario activo para la transformación del consumidor.

    El estado de la dependencia con la perspectiva productora 'calendar' viaja en
    `parametros['calendar']`.

    Dos casos:

    1) Ejecución sin productor: no existe la clave 'calendar' en parametros.
       Se descubre un calendario propio en cada llamada.

    2) Productor acoplado: el orquestador garantiza que el consumer no se lanza hasta
       que el productor 'calendar' haya producido al menos un modelo. Se drena la
       cola promoviendo a `calendario_actual` cada entrada cuya `validez` ya esté
       superada por `fecha_inicio_ventana`. El `while` permite saltar varios
       calendarios obsoletos de una vez si el consumer llevaba ticks sin sincronizarse.
    """
    estado_dep = parametros.get('calendar')

    # --- Caso 1: sin productor 'calendar'. ---
    if estado_dep is None:
        # Hacer singleton el calendario descubierto para evitar recalcularlo en cada iteración del consumidor.
        if getattr(_obtener_calendario, 'calendario_estatico', None) is None:
            logger.info("Sin dependencia 'calendar' acoplada: descubriendo calendario propio (primera iteración).")
            _obtener_calendario.calendario_estatico = _extraer_calendario(log, parametros)
        else:
            logger.debug("Reutilizando calendario propio descubierto previamente.")
        return _obtener_calendario.calendario_estatico

    # --- Caso 2: productor acoplado. ---
    calendarios_pendientes = estado_dep['calendarios_pendientes']
    calendario             = estado_dep.get('calendario_actual')
    fecha_inicio_ventana   = parametros.get('inicio', None)

    # validez None marca el primer modelo del productor ("válido desde siempre").
    while calendarios_pendientes and (
        calendarios_pendientes[0]['validez'] is None
        or fecha_inicio_ventana > calendarios_pendientes[0]['validez']
    ):
        logger.info(f"Usando un calendario nuevo desde la cola (fecha validez {calendarios_pendientes[0]['validez']}).")
        # El productor publica su modelo completo ({'modelo': lista de calendarios fuzzy, ...});
        # los consumidores trabajan con la representación tabular de intervalos.
        modelo_productor = calendarios_pendientes.popleft()['calendario']
        calendario = _calendario_a_dataframe(modelo_productor['modelo'])
        estado_dep['calendario_actual'] = calendario  # persistencia por referencia entre iteraciones
        parametros['estado_cambiado_por_dependencia'] = True  # marcar que el estado ha cambiado para que el orquestador lo sepa

    return calendario
            
def _extraer_calendario(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Descubre el calendario de cada recurso a partir del log y devuelve una representación tabular
    de los intervalos de trabajo. Pensado para perspectivas que necesitan el calendario como dato auxiliar
    (resource utilization, resource productivity), sin entrar en la detección de cambios de los calendarios.

    Recibe el log en nomenclatura XES. Internamente lo renombra al esquema que espera
    pix-framework (que usa itertuples() y no admite ':' en los nombres de atributo).
    """

    # XES -> esquema pix-framework (necesario para itertuples() dentro de pix-framework)
    log_pix = log.rename(columns=_XES_A_PIX)

    # Descubrir el calendario de los recursos usando pix-framework (modelo fuzzy)
    resource_calendar, _ = discovery_fuzzy_resource_calendars_and_performances(
        log_pix,
        log_ids=_PIX_IDS,
        granularity=60  # granularidad en minutos (por defecto, 1 hora)
    )

    return _calendario_a_dataframe(resource_calendar)

def _calendario_a_dataframe(resource_calendar) -> pd.DataFrame:
    """
    Convierte la lista de calendarios fuzzy de pix-framework (el formato en el que la
    perspectiva 'calendar' publica su modelo) a la representación tabular de intervalos
    por recurso que consumen _calcular_TPA y las métricas de resource profiles.
    """

    # Volcar los intervalos de cada recurso a una lista de diccionarios para construir el DataFrame
    calendarios = []
    for rc in resource_calendar:
        resource_name = rc.resource_name
        intervals = rc.intervals
        for interval in intervals:
            calendario = {
                'from_day': interval.from_day,
                'to_day': interval.to_day,
                'start_time': interval._start_time,
                '_end_time': interval._end_time,
                'probability': interval.probability,
                'resource_name': resource_name
            }
            calendarios.append(calendario)

    df_calendarios = pd.DataFrame(calendarios)

    # Reemplazar el fin "00:00:00" (medianoche) por "23:59:59" para que el intervalo cubra el día completo.
    df_calendarios['_end_time'] = df_calendarios['_end_time'].astype(str).str.replace('00:00:00', '23:59:59')

    return df_calendarios

def _calcular_TPA(start: pd.Timestamp, end: pd.Timestamp, resource: str, calendario: pd.DataFrame, DEBUG: bool = False) -> float:
    """
    Calcula el TPA (Processing time when resource is Available) para un evento.

    Recorre todos los días que abarca el evento e intersecta el intervalo [start, end] con los bloques
    laborables del recurso según el calendario. El calendario se trata como un patrón semanal (la fecha
    almacenada en start_time/_end_time es arbitraria, sólo importa la hora del día), por lo que cada
    bloque se "instancia" en la fecha real de cada día del bucle antes de intersectar.

    Args:
        start: Marca temporal de inicio del evento.
        end: Marca temporal de fin del evento.
        resource: Nombre del recurso que ejecuta el evento.
        calendario: DataFrame con el calendario de los recursos (salida de _extraer_calendario).

    Returns:
        Tiempo efectivo trabajado (en segundos) descontando los huecos no laborables.
    """

    # Filtrar el calendario para el recurso del evento
    #print(resource)
    cal_recurso = calendario[calendario['resource_name'] == resource]

    #print(start)
    #print(end)

    if cal_recurso.empty:
        logger.info(f"No hay calendario para el rescurso {resource}, se devuelve 0.0 en el TPA")
        return 0.0  # Si no hay calendario para el recurso o el intervalo es inválido
    
    if start >= end:
        logger.info(f"Inicio {start} y fin {end} de tareas inválidos para el rescurso {resource}, se devuelve 0.0 en el TPA")
        return 0.0  # Si no hay calendario para el recurso o el intervalo es inválido

    tiempo_total_efectivo = 0.0

    # Bucle externo: recorrer todos los días que abarca el evento (start_date -> end_date)
    dia = start.normalize()  # 00:00:00 del día de inicio del evento

    while dia <= end.normalize():

        # Día de la semana del día actual (e.g. "MONDAY") para emparejar con el patrón del calendario
        weekday = WEEKDAYS[dia.weekday()]

        # Sólo los bloques del calendario cuyo patrón aplica a este día
        bloques_dia = cal_recurso[cal_recurso['from_day'] == weekday]

        # Bucle interno: intersección del evento con cada bloque del calendario
        for _, bloque in bloques_dia.iterrows():

            # Instanciar el bloque: combinar la fecha real (dia) con la hora del patrón (start_time / _end_time)
            t_ini = pd.Timestamp(bloque['start_time']).time()
            t_fin = pd.Timestamp(bloque['_end_time']).time()
            # Si _end_time queda en 00:00 (caso 24/7, donde el .str.replace de _extraer_calendario no
            # ha encontrado el "00:00:00" porque pandas omite la hora al stringificar una columna que
            # contiene SOLO medianoches), forzar 23:59:59 para que inicio y fin del bloque no coincidan.
            if t_fin == time(0, 0, 0):
                t_fin = time(23, 59, 59)
            bloque_inicio = dia.replace(hour=t_ini.hour, minute=t_ini.minute, second=t_ini.second)
            bloque_fin = dia.replace(hour=t_fin.hour, minute=t_fin.minute, second=t_fin.second)

            # Intersección del intervalo del evento con el bloque del calendario
            inicio_real = max(start, bloque_inicio)
            fin_real = min(end, bloque_fin)

            if inicio_real < fin_real:
                tiempo_total_efectivo += (fin_real - inicio_real).total_seconds()

        # Avanzar al siguiente día
        dia += pd.Timedelta(days=1)

    #if DEBUG:
        #logger.debug(f"Tiempo efectivo para el evento ejecutado por {resource}: {tiempo_total_efectivo} segundos")

    return tiempo_total_efectivo

def _resource_productivity_per_pair(resource: str, task: str, ventana: pd.DataFrame, DEBUG: bool = False) -> list[float]:
    """
    Calcula la productividad de un recurso para una tarea específica.
    """

    if DEBUG:
        logger.debug(f"Evaluando la productividad para el recurso {resource} y la tarea {task}")

    lista_productividad = []

    # Tiempo promedio para realizar la tarea (incluyendo todos los recursos)
    eventos_tarea = ventana[ventana['concept:name'] == task]
    TPA_medio_eventos_tarea = eventos_tarea['TPA'].mean()

    if DEBUG:
        logger.debug(f"TPA promedio para la tarea {task}: {TPA_medio_eventos_tarea} segundos")

    # Eventos de la tarea realizados por el recurso
    eventos_tareas_recurso = ventana[(ventana['org:resource'] == resource) & (ventana['concept:name'] == task)]

    for _, evento in eventos_tareas_recurso.iterrows():

        #if DEBUG:
            #logger.debug(f"Evento de la tarea {task} realizado por el recurso {resource}: TPA={evento['TPA']}")

        productividad = evento['TPA'] / TPA_medio_eventos_tarea if TPA_medio_eventos_tarea > 0 else 0.0

        lista_productividad.append(productividad)

    return lista_productividad

def _resource_productivity(ventana: pd.DataFrame, DEBUG: bool = False) -> dict[tuple[str, str], list[float]]:
    """
    Calcula el performance deviation ratio para todos los pares (recurso, tarea) de la ventana.

    Args:
        ventana: DataFrame con la ventana de datos.
        DEBUG: Flag para activar el modo de depuración.

    Returns:
        El valor medio de la productividad (TPA del recurso / TPA medio de la tarea) como un float.
    """

    recursos = ventana['org:resource'].unique()
    tareas = ventana['concept:name'].unique()
    
    combinaciones = list(itertools.product(recursos, tareas))

    resource_productivity = {}

    for resource, task in combinaciones:

        # Calcular la productividad para el par (recurso, tarea) usando la función _resource_productivity_per_pair
        productividad_recurso_tarea = _resource_productivity_per_pair(resource, task, ventana, DEBUG)

        if DEBUG:
            logger.debug(f"Recurso: {resource}, Tarea: {task}, Productividad: {productividad_recurso_tarea}")

        resource_productivity[(resource, task)] = productividad_recurso_tarea

        if DEBUG:
            logger.info(f"Cantidad de muestras para el par {resource, task}: {len(productividad_recurso_tarea)}")

    if DEBUG:
        logger.debug(f"Productividad por recurso y tarea: {resource_productivity}")

    return resource_productivity

def transformacion_resource_productivity(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Calcula el tiempo de procesamiento (TPA) por evento, necesario para el cálculo de resource productivity.
    """

    DEBUG = parametros.get('debug', False)

    # Preparar el log para pix-framework y descubrir el calendario de los recursos (dato auxiliar para TPA)
    log = _preparar_log_calendarios(log, parametros)

    # Hacer singelton el calendario
    calendario = _obtener_calendario(log, parametros)

    ###################
    # Calculo del TPA #
    ###################
    log['TPA'] = log.apply(lambda ev: _calcular_TPA(
        ev[DEFAULT_XES_IDS.start_time], ev[DEFAULT_XES_IDS.end_time], ev[DEFAULT_XES_IDS.resource], calendario, DEBUG
    ), axis=1)

    if DEBUG:
        log.to_csv(f"./data/03_primary/resource_productivity_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_con_TPA.csv", index=False)

    ###############################
    # Calculo de la productividad #
    ###############################
    rp = _resource_productivity(log, DEBUG)

    # Creación del log con los valores de productividad
    log_transformado = pd.DataFrame([(recurso, tarea, prod) for (recurso, tarea), prod in rp.items()],
                                    columns=['org:resource', 'concept:name', 'resource_productivity'])

    if DEBUG:
        calendario.to_csv(f"./data/06_models/calendario_para_productividad_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.csv", index=False)
        log_transformado.to_csv(f"./data/03_primary/resource_productivity_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_transformado.csv", index=False)

    return log_transformado

def transformacion_resource_utilization(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Calcula el tiempo de procesamiento (TP) por evento, necesario para el cálculo del
    resource utilization index.
    """

    DEBUG = parametros.get('debug', False)

    log['TP'] = (pd.to_datetime(log[DEFAULT_XES_IDS.end_time], utc=True) - pd.to_datetime(log[DEFAULT_XES_IDS.start_time], utc=True)).dt.total_seconds()

    if DEBUG:
        log.to_csv(f"./data/03_primary/resource_utilization_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_transformado.csv", index=False)

    return log

# --- TAREAS DE DESCUBRIMIENTO DE MODELO ---

def _encontrar_mejor_distribucion(datos: pd.DataFrame, recurso: str, tarea: str, familias_candidatas: list, DEBUG: bool = False) -> Optional[Dict[str, Any]]:
    """
    Ajusta cada familia candidata por MLE y elige la mejor por AIC (no por KS).

    Dos cambios clave respecto a la versión anterior:
    - Para familias con soporte [0, inf) se fija floc=0 en el fit. Si se deja libre,
      el parámetro de localización absorbe sesgos y cualquier familia gana
      artificialmente (problema clásico al ajustar distribuciones desplazadas).
    - Selección por AIC en lugar de estadístico KS. El KS sobre los mismos datos
      con los que se ajustó la distribución (Lilliefors) está sesgado a la baja
      y favorece familias con más parámetros libres. AIC penaliza el número de
      parámetros y es el criterio correcto para *comparar* familias entre sí.

    `familias_candidatas` es una lista de objetos scipy.stats ya resueltos (ver
    `_resolver_familias_candidatas`). Las familias con soporte estrictamente positivo
    que requieren floc=0 están en `_FAMILIAS_SOPORTE_POSITIVO`; si el usuario añade
    familias custom con soporte en [0, inf), no se les fijará floc=0 — el ajuste
    seguirá funcionando, pero el AIC podría ser menos comparable.
    """

    mejor_familia = None
    mejor_aic     = float('inf')

    for distribucion in familias_candidatas:

        # Algunas familias (lognorm, gamma) tienen soporte estrictamente
        # positivo o pueden converger fuera del rango admisible (FitError). Si el ajuste
        # falla, se omite la familia y se sigue evaluando el resto.
        try:

            # A. Entrenar la familia con los datos.
            # Para familias con soporte positivo, fijamos loc=0 para evitar el
            # parámetro de desplazamiento artificial.
            if distribucion.name in _FAMILIAS_SOPORTE_POSITIVO:
                parametros = distribucion.fit(datos, floc=0)
            else:
                parametros = distribucion.fit(datos)

            # B. Calcular el AIC del ajuste.
            # AIC = 2k - 2·log(L), donde k es el número de parámetros LIBRES
            # (los que realmente se han optimizado, no los que se han fijado).
            log_verosimilitud = np.sum(distribucion.logpdf(datos, *parametros))
            n_param_libres    = len(parametros) - (1 if distribucion.name in _FAMILIAS_SOPORTE_POSITIVO else 0)
            aic               = 2 * n_param_libres - 2 * log_verosimilitud

        except Exception as e:
            if DEBUG:
                logger.debug(f"Familia {distribucion.name:<12} | NO AJUSTABLE ({e.__class__.__name__})")
            continue

        if DEBUG:
            logger.debug(f"Familia {distribucion.name:<12} | AIC: {aic:.2f}")

        # C. Guardar la ganadora (AIC más bajo).
        if aic < mejor_aic:
            mejor_aic     = aic
            mejor_familia = distribucion
            mejores_parametros = parametros

    if mejor_familia is None:
        if DEBUG:
            logger.debug(f"Ninguna familia se ajusta a los datos de Recurso='{recurso}' y Tarea='{tarea}'.")
        return None

    
    logger.info(f"LA FAMILIA GANADORA ES: '{mejor_familia.name}', recurso='{recurso}', tarea='{tarea}' con AIC={mejor_aic:.2f}")

    return {'familia': mejor_familia, 'parametros': mejores_parametros}

def modelo_resource_productivity(log: pd.DataFrame, parametros: dict, random_state: int = 42) -> dict:
    """
    Descubre la distribución de referencia de productividad para cada par (recurso, tarea)
    a partir del log ya transformado (una fila por par con la lista de productividades en
    la columna 'resource_productivity').

    Se prueban varias familias paramétricas candidatas (por defecto: norm, lognorm, gamma,
    expon, uniform; configurable vía `parametros['familias_candidatas']` como lista de
    nombres de `scipy.stats`) y se elige la mejor por AIC delegando en
    `_encontrar_mejor_distribucion`.

    Soporta redescubrimiento parcial: si `parametros` contiene las claves internas
    `_recursos_a_redescubrir` (lista de tuplas `(recurso, tarea)`) y `_modelo_anterior`
    (dict con el modelo previo), refit SOLO esos pares y conserva las referencias del
    resto tal cual estaban en el modelo anterior. Los pares solicitados para los que
    el log no aporta muestras o cuyo refit falla son eliminados del modelo combinado
    (el detector dejará de comparar contra ellos).

    Sin esas claves, descubrimiento completo: se recorre el producto cartesiano de
    recursos × tareas presentes en el log (comportamiento original).

    Returns:
        {'modelo': {(recurso, tarea): {'familia': dist, 'parametros': params}}}.
    """

    DEBUG = parametros.get('debug', False)

    # Familias candidatas. Si el YAML no especifica
    # `familias_candidatas`, se usa la lista por defecto definida en `_FAMILIAS_DEFAULT`.
    familias_candidatas = _resolver_familias_candidatas(parametros.get('familias_candidatas'))

    pares_a_redescubrir = parametros.get('_recursos_a_redescubrir')
    modelo_anterior = parametros.get('_modelo_anterior')
    rebuild_parcial = bool(pares_a_redescubrir) and modelo_anterior is not None

    if rebuild_parcial:
        logger.info(
            f"Redescubriendo distribuciones solo para los pares solicitados: {pares_a_redescubrir}"
        )
        # Partimos del modelo anterior conservando las referencias de los pares NO afectados.
        pares_set = set(pares_a_redescubrir)
        distribuciones_recurso_tarea = {
            par: ref for par, ref in modelo_anterior['modelo'].items()
            if par not in pares_set
        }
        combinaciones = list(pares_a_redescubrir)
    else:
        logger.info("Ajustando automáticamente la mejor distribución temporal para cada par recurso-tarea...")
        distribuciones_recurso_tarea = {}
        # Extraemos los recursos y tareas únicos para generar combinaciones.
        recursos = log[DEFAULT_XES_IDS.resource].unique()
        tareas = log[DEFAULT_XES_IDS.activity].unique()
        combinaciones = list(itertools.product(recursos, tareas))

    for recurso, tarea in combinaciones:

        if DEBUG:
            logger.debug(f"Evaluando combinación: Recurso='{recurso}' | Tarea='{tarea}'")

        sublog = log[(log[DEFAULT_XES_IDS.resource] == recurso) & (log[DEFAULT_XES_IDS.activity] == tarea)]
        if sublog.empty:
            logger.info(f"  - No hay datos para el par ({recurso}, {tarea}); se omite.")
            continue

        # Cada fila de log_transformado guarda la lista de productividades del par (recurso, tarea)
        # en 'resource_productivity'. .values devolvería un array (1, dtype=object) que scipy no
        # puede pasar por np.isfinite, así que se desempaqueta la lista y se castea a float.
        datos = np.asarray(sublog['resource_productivity'].iloc[0], dtype=float)

        if len(datos) == 0:
            logger.info(f"  - Sin muestras para el par ({recurso}, {tarea}); se omite.")
            continue

        # Winsorizar para limitar el peso de los outliers en el ajuste.
        datos = _winsorizar(datos)

        # Se ajusta automáticamente la mejor distribución entre las familias candidatas
        distribuciones_recurso_tarea[(recurso, tarea)] = _encontrar_mejor_distribucion(datos, recurso, tarea, familias_candidatas, DEBUG)

    # Guardar siempre la información del modelo (distribución elegida por par) en la capa de modelos.
    ruta_modelo = f"./data/06_models/Productividad_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.csv"
    filas_modelo = [
        {'recurso': recurso, 'tarea': tarea,
         'familia': referencia['familia'].name,
         'parametros': referencia['parametros']}
        for (recurso, tarea), referencia in distribuciones_recurso_tarea.items()
        if referencia is not None
    ]
    pd.DataFrame(filas_modelo).to_csv(ruta_modelo, index=False)
    logger.info(f"Modelo de productividad guardado en {ruta_modelo}")

    return {'modelo': distribuciones_recurso_tarea}


import itertools
import logging
from datetime import time


import pandas as pd
import numpy as np
import pm4py
import scipy.stats as st

from typing import Optional, Tuple, Dict, Any
from sklearn.metrics import mean_absolute_error, mean_squared_error
from .perspectivas.arrival_rate import preprocesado_arrival_rate
from .perspectivas.service_rate import preprocesado_service_rate
from .perspectivas import resource_profiles
from .logging_config import get_logger
from pix_framework.discovery.resource_calendar_and_performance.fuzzy.discovery import FuzzyResourceCalendar

logger = get_logger(__name__)

def calcular_fitness(log: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> float:
    """
    Calcula el valor de fitness del modelo con respecto al log de eventos.

    Args:
        log: DataFrame con el log de eventos.
        modelo: Diccionario que contiene la Red de Petri ('net', 'initial_marking', 'final_marking').

    Returns:
        El fitness del log como un valor flotante (log_fitness).
    """

    DEBUG = config.get('debug', False)

    net = modelo['net']
    initial_marking = modelo['initial_marking']
    final_marking = modelo['final_marking']

    # Ejecutar el replay basado en tokens para obtener las métricas.
    resultado = pm4py.fitness_token_based_replay(log, net, initial_marking, final_marking)

    #if DEBUG:
        #logger.debug(f"Percentage of fit traces: {resultado['percentage_of_fitting_traces']:.2f}%")
        #logger.debug(f"Average trace fitness: {resultado['average_trace_fitness']:.4f}")
        #logger.debug(f"Log fitness: {resultado['log_fitness']:.4f}")

    resultado = resultado['perc_fit_traces'] / 100.0

    logger.info(f"Calculando fitness (Percentage of fitting traces): {resultado:.2f}")

    return resultado

def calcular_precision(log: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> float:
    """
    Calcula la precisión del modelo con respecto al log de eventos.

    Args:
        log: DataFrame con el log de eventos.
        modelo: Diccionario que contiene la Red de Petri ('net', 'initial_marking', 'final_marking', 'OLP').

    Returns:
        La precisión del modelo como un valor flotante.
    """

    DEBUG = config.get('debug', False)

    # Calcular DFR
    dfg, _, _ = pm4py.discover_dfg(log)
    DFR = set(dfg.keys())

    OLP = modelo['OLP']

    # Calcular precision
    if len(OLP) == 0:
        return 0.0
        
    # |OLP \ DFR| -> Caminos que están en el modelo pero NO en el log
    caminos_perdidos = OLP - DFR
    
    precision = 1.0 - (len(caminos_perdidos) / len(OLP))

    if DEBUG:
        logger.debug(f"OLP: {OLP}")
        logger.debug(f"Longitud OLP: {len(OLP)}")
        logger.debug(f"DFR: {DFR}")
        logger.debug(f"Longitud DFR: {len(DFR)}")
        logger.debug(f"|OLP \ DFR|: {caminos_perdidos}")
        logger.debug(f"Longitud |OLP \ DFR|: {len(caminos_perdidos)}")

    logger.info(f"Calculando precisión: {precision:.2f}")

    return precision

def calcular_metrica_modelo_sklearn(ventana: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> float:
    """
    Calcula una métrica sobre el modelo con respecto a la ventana de datos.
    Args:
        ventana: DataFrame con la ventana de datos.
        modelo: Diccionario con el modelo entrenado.
        config: Diccionario con la configuración.
        metrica: Nombre de la métrica a calcular (e.g., 'RMSE', 'MAE').
    Returns:
        El valor de la métrica como un float.
    """

    perpsectiva = modelo['objective']

    # Preprocesar la ventana de datos según la perspectiva del modelo
    test_preprocesado = None
    if perpsectiva == 'arrival_rate':
        test_preprocesado = preprocesado_arrival_rate(ventana, config, verbose=logging.DEBUG, test=True, features=modelo['features'])
    elif perpsectiva == 'service_rate':
        test_preprocesado = preprocesado_service_rate(ventana, config, verbose=logging.DEBUG, test=True, features=modelo['features'])

    if test_preprocesado is not None:
        # Llamar a la función de cálculo de la métrica correspondiente
        if metrica == 'MAE':  
            return MAE(test_preprocesado, modelo, config)
        elif metrica == 'MSE':
            return MSE(test_preprocesado, modelo, config)
        else:
            logger.error(f"Métrica desconocida: {metrica}")
            raise ValueError(f"Métrica desconocida: {metrica}")

def MSE(ventana: pd.DataFrame, modelo: dict, config: dict):
    """
    Calcula el Mean Squared Error (MSE) entre los valores verdaderos y las predicciones.

    Args:
        ventana: DataFrame con la ventana de datos.
        modelo: Diccionario con el modelo entrenado.
        config: Diccionario con la configuración.

    Returns:
        El valor de MSE como un float.
    """
    DEBUG = config.get('debug', False)
    y = modelo['objective']
    logger.info(f"Calculando MSE para el modelo de {y}")

    test = ventana[modelo['features'] + [y]].copy()

    y_test = test[y]
    x_test = test[modelo['features']]

    y_pred = modelo['modelo'].predict(x_test)

    # Dado que la arrival rate es un conteo de eventos, redondeamos las predicciones al entero más cercano para que tengan sentido en este contexto.
    # y_pred = np.round(y_pred).astype(int)

    mse = mean_squared_error(y_test, y_pred)

    if DEBUG:
        logger.debug(f"Valores reales (y_test): {y_test.values}")
        logger.debug(f"Predicciones del modelo (y_pred): {y_pred}")
        pd.DataFrame({'real': y_test.values, 'prediccion': y_pred}).to_csv(
            f"./data/07_model_output/{y}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_predicciones.csv", index=False)

    logger.info(f"MSE: {mse:.4f}")

    return mse

def MAE(ventana: pd.DataFrame, modelo: dict, config: dict):
    """
    Calcula el Mean Absolute Error (MAE) entre los valores verdaderos y las predicciones.

    Args:
        ventana: DataFrame con la ventana de datos.
        modelo: Diccionario con el modelo entrenado.
        config: Diccionario con la configuración.

    Returns:
        El valor de MAE como un float.
    """

    DEBUG = config.get('debug', False)
    y = modelo['objective']
    logger.info(f"Calculando MAE para el modelo de {y}")

    test = ventana[modelo['features'] + [y]].copy()

    y_test = test[y]
    x_test = test[modelo['features']]

    y_pred = modelo['modelo'].predict(x_test)

    # Dado que la arrival rate es un conteo de eventos, redondeamos las predicciones al entero más cercano para que tengan sentido en este contexto.
    y_pred = np.round(y_pred).astype(int)

    mae = mean_absolute_error(y_test, y_pred)

    if DEBUG:
        logger.debug(f"Valores reales (y_test): {y_test.values}")
        logger.debug(f"Predicciones del modelo (y_pred): {y_pred}")
        pd.DataFrame({'real': y_test.values, 'prediccion': y_pred}).to_csv(
            f"./data/07_model_output/{y}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_predicciones.csv", index=False)

    logger.info(f"MAE: {mae:.4f}")

    return mae

def _convert_string_to_weekday(weekday_str: str) -> int:
    """
    Convierte un string con el nombre del día de la semana a su representación numérica (0-6, donde 0 es lunes y 6 es domingo).

    Args:
        weekday_str: Nombre del día de la semana (e.g., 'MONDAY', 'TUESDAY', etc.).
    Returns:
        Un entero que representa el día de la semana (0-6).
    """
    mapping = {
        'MONDAY': 0,
        'TUESDAY': 1,
        'WEDNESDAY': 2,
        'THURSDAY': 3,
        'FRIDAY': 4,
        'SATURDAY': 5,
        'SUNDAY': 6
    }
    return mapping.get(weekday_str, -1)  # Devuelve -1 si el string no es válido

def support_per_resource(df_recurso: pd.DataFrame, resource_calendar: FuzzyResourceCalendar, verbose: bool) -> Dict[str, float]:

    df_recurso['weekday'] = df_recurso['start_timestamp'].dt.dayofweek
    df_recurso['time'] = df_recurso['start_timestamp'].dt.time
    
    total_eventos = len(df_recurso)
    eventos_cubiertos = 0

    for index, evento in df_recurso.iterrows():
        
        if verbose:
            logger.debug(f"Evaluando evento: weekday={evento['weekday']}, time={evento['time']}")

        cubierto = False
        
        intervals = resource_calendar.intervals
        for i in intervals:

            week_day = False
            start_time = False
            end_time = False

            from_day_convertido = _convert_string_to_weekday(i.from_day)
            to_day_convertido = _convert_string_to_weekday(i.to_day)

            #if verbose:
            #    logger.debug(f"Intervalo: {from_day_convertido} - {to_day_convertido} {i._start_time} - {i._end_time}")

            if evento['weekday'] == from_day_convertido or evento['weekday'] == to_day_convertido:
                week_day = True
                start_time_convertido = i._start_time.time()
                end_time_convertido = i._end_time.time()

                if end_time_convertido == time(0, 0, 0):
                    end_time_convertido = time(23, 59, 59)

            if week_day:

                if start_time_convertido <= evento['time']:
                    start_time = True
                    
                if end_time_convertido >= evento['time']:
                    end_time = True

            #if verbose:
                #logger.debug(f" => week_day: {week_day}, start_time: {start_time}, end_time: {end_time}")

            if week_day and start_time and end_time:
                if verbose:
                    logger.debug(f"Evento cubierto por el intervalo: {i.from_day} - {i.to_day} {start_time_convertido} - {end_time_convertido}")
                eventos_cubiertos += 1
                cubierto = True
                break 

        if not cubierto:
            if verbose:
                logger.debug(f"Evento no cubierto")
        else:
            cubierto = False

    if verbose:
        logger.debug(f"Total eventos para el recurso: {total_eventos}")
        logger.debug(f"Eventos cubiertos por el calendario: {eventos_cubiertos}")

    soporte_recurso = eventos_cubiertos / total_eventos if total_eventos > 0 else 0.0
    
    return soporte_recurso

def support_per_resource_inverted(df_recurso: pd.DataFrame, resource_calendar: FuzzyResourceCalendar, verbose: bool) -> Dict[str, float]:

    df_recurso['weekday'] = df_recurso['start_timestamp'].dt.dayofweek
    df_recurso['time'] = df_recurso['start_timestamp'].dt.time

    intervals = resource_calendar.intervals
    total_intervalos = len(intervals)
    intervalos_cubiertos = 0
    
    cubierto = False

    for i in intervals:

        start_time_convertido = i._start_time.time()
        end_time_convertido = i._end_time.time()

        if end_time_convertido == time(0, 0, 0):
                    end_time_convertido = time(23, 59, 59)

        if verbose:
            logger.debug(f"Evaluando intervalo: {i.from_day} - {i.to_day} {start_time_convertido} - {end_time_convertido}")

        for index, evento in df_recurso.iterrows():
            
            week_day = False
            start_time = False
            end_time = False

            from_day_convertido = _convert_string_to_weekday(i.from_day)
            to_day_convertido = _convert_string_to_weekday(i.to_day)

            #if verbose:
            #    logger.debug(f"Evento: weekday={evento['weekday']}, time={evento['time']}")

            if evento['weekday'] == from_day_convertido or evento['weekday'] == to_day_convertido:
                week_day = True
                
            if week_day:

                if start_time_convertido <= evento['time']:
                    start_time = True
                    
                if end_time_convertido >= evento['time']:
                    end_time = True

            #if verbose:
            #    logger.debug(f" => week_day: {week_day}, start_time: {start_time}, end_time: {end_time}")

            if week_day and start_time and end_time:
                if verbose:
                    logger.debug(f"Intervalo cubierto por el evento: weekday={evento['weekday']}, time={evento['time']}")
                intervalos_cubiertos += 1
                cubierto = True
                break 
        
        if not cubierto:
            if verbose:
                logger.debug(f"Intervalo no cubierto")
        else:
            cubierto = False

    if verbose:
        logger.debug(f"Total intervalos cubiertos para el recurso: {total_intervalos}")
        logger.debug(f"Intervalos cubiertos por el calendario: {intervalos_cubiertos}")

    soporte_recurso = intervalos_cubiertos / total_intervalos if total_intervalos > 0 else 0.0
    
    return soporte_recurso

def calcular_support(ventana: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> Dict[str, float]:
    """
    Calcula el soporte por recurso para el modelo de calendarios.

    Devuelve un diccionario {recurso: soporte}. Los recursos ausentes en la ventana
    se omiten (no se inventa una entrada): el detector interpretará esa ausencia
    como "racha congelada" y no actualizará el historial del recurso.

    Args:
        ventana: DataFrame con la ventana de datos.
        modelo: Diccionario con el modelo entrenado (lista de calendarios por recurso).
        config: Diccionario con la configuración.
        metrica: Nombre de la métrica a calcular.

    Returns:
        Dict[str, float] con el soporte por recurso. Vacío si ningún recurso del
        modelo aparece en la ventana.
    """

    DEBUG = config.get('debug', False)

    resource_calendar = modelo['modelo']
    recursos = modelo['recursos']

    support_por_recurso: Dict[str, float] = {}
    for r in recursos:
        df_r = ventana[ventana['org:resource'] == r].copy()
        if df_r.empty:
            continue
        resource_calendar_r = None
        for rc in resource_calendar:
            if rc.resource_name == r:
                resource_calendar_r = rc
                break
        if resource_calendar_r is None:
            logger.warning(f"Recurso {r} de la ventana no encontrado en el calendario: se omite para soporte")
            continue
        support_r = support_per_resource(df_r, resource_calendar_r, DEBUG)
        logger.info(f"Recurso: {r}, Soporte: {support_r:.4f}")
        support_por_recurso[r] = support_r

    if support_por_recurso:
        media = float(np.mean(list(support_por_recurso.values())))
        logger.info(f"Soporte por recurso ({len(support_por_recurso)} recursos), media: {media:.4f}")
    else:
        logger.info("Soporte: ningún recurso del modelo está presente en la ventana.")

    return support_por_recurso

def calcular_inverted_support(ventana: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> Dict[str, float]:
    """
    Calcula el soporte invertido por recurso para el modelo de calendarios.

    Devuelve un diccionario {recurso: soporte_invertido}. Recursos ausentes en la
    ventana se omiten para que el detector congele su racha en lugar de romperla.

    Args:
        ventana: DataFrame con la ventana de datos.
        modelo: Diccionario con el modelo entrenado.
        config: Diccionario con la configuración.
        metrica: Nombre de la métrica a calcular.

    Returns:
        Dict[str, float] con el soporte invertido por recurso.
    """

    DEBUG = config.get('debug', False)

    resource_calendar = modelo['modelo']
    recursos = modelo['recursos']

    inverted_por_recurso: Dict[str, float] = {}
    for r in recursos:
        df_r = ventana[ventana['org:resource'] == r].copy()
        if df_r.empty:
            continue
        resource_calendar_r = None
        for rc in resource_calendar:
            if rc.resource_name == r:
                resource_calendar_r = rc
                break
        if resource_calendar_r is None:
            logger.warning(f"Recurso {r} de la ventana no encontrado en el calendario: se omite para soporte invertido")
            continue
        inverted_support_r = support_per_resource_inverted(df_r, resource_calendar_r, DEBUG)
        logger.info(f"Recurso: {r}, Soporte invertido: {inverted_support_r:.4f}")
        inverted_por_recurso[r] = inverted_support_r

    if inverted_por_recurso:
        media = float(np.mean(list(inverted_por_recurso.values())))
        logger.info(f"Soporte invertido por recurso ({len(inverted_por_recurso)} recursos), media: {media:.4f}")
    else:
        logger.info("Soporte invertido: ningún recurso del modelo está presente en la ventana.")

    return inverted_por_recurso

def _resource_colab_per_pair(recurso_1: str, recurso_2: str, ventana: pd.DataFrame, DEBUG: bool) -> float:

    # Obtener los casos de cada ventana
    cases = ventana['case:concept:name'].unique()

    num = 0

    for c in cases:

        df_caso = ventana[ventana['case:concept:name'] == c].copy()

        recursos_caso = df_caso['org:resource'].unique()

        if recurso_1 in recursos_caso and recurso_2 in recursos_caso:

            if DEBUG:
                logger.debug(f"Caso {c} tiene ambos recursos: {recurso_1} y {recurso_2}")

            num = num + 1

    return num / len(cases) if len(cases) > 0 else 0.0

def resource_colab(ventana: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> Dict[str, float]:
    """
    """
    
    DEBUG = config.get('debug', False)

    recursos = ventana['org:resource'].unique()

    combinaciones = list(itertools.combinations(recursos, 2))

    resultados_colab = {}

    for c in combinaciones:
        recurso_1 = c[0]
        recurso_2 = c[1]

        # Calcular la colaboración entre recurso_1 y recurso_2 usando la función resource_colab_per_pair
        colab = _resource_colab_per_pair(recurso_1, recurso_2, ventana, DEBUG)

        if DEBUG:
            logger.debug(f"Recursos: {recurso_1} - {recurso_2}, Colaboración: {colab:.4f}")

        resultados_colab[(recurso_1, recurso_2)] = colab

    if DEBUG:
        logger.debug(f"Colaboración por pares de recursos: {resultados_colab}")

    resource_colab = np.mean(list(resultados_colab.values()))

    logger.info(f"Colaboración entre recursos: {resource_colab:.4f}")

    return resource_colab

def _resource_skill_per_pair(resource: str, task: str, ventana: pd.DataFrame, DEBUG: bool) -> float:
    """
    Calcula la skill de un recurso para una tarea específica como el número de instancias completadas.
    """

    eventos_tareas_recurso = ventana[(ventana['org:resource'] == resource) & (ventana['concept:name'] == task)]

    return len(eventos_tareas_recurso)

def resource_skill(ventana: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> float:
    """
    Calcula el instance count para todos los pares (recurso, tarea) de la ventana.

    Args:
        ventana: DataFrame con la ventana de datos.
        modelo: Diccionario con el modelo entrenado.
        config: Diccionario con la configuración.
        metrica: Nombre de la métrica a calcular.

    Returns:
        El número medio de instancias por par (recurso, tarea) como un float.
    """

    DEBUG = config.get('debug', False)

    recursos = ventana['org:resource'].unique()
    tareas = ventana['concept:name'].unique()

    combinaciones = list(itertools.product(recursos, tareas))

    resultados_skill = {}

    for resource, task in combinaciones:

        # Calcular la skill para el par (recurso, tarea) usando la función _resource_skill_per_pair
        skill = _resource_skill_per_pair(resource, task, ventana, DEBUG)

        if DEBUG:
            logger.debug(f"Recurso: {resource}, Tarea: {task}, Skill: {skill:.4f}")

        resultados_skill[(resource, task)] = skill

    if DEBUG:
        logger.debug(f"Skill por recurso y tarea: {resultados_skill}")

    resource_skill = np.mean(list(resultados_skill.values())) if resultados_skill else 0.0

    logger.info(f"Skill de los recursos: {resource_skill:.4f}")

    return resource_skill

def _resource_utilization_per_resource(resource: str, ventana: pd.DataFrame, calendario: pd.DataFrame, tau_min: pd.Timestamp, tau_max: pd.Timestamp, DEBUG: bool) -> float:
    """
    Calcula el resource utilization index para un recurso específico.

    El numerador es la suma de TP (processing time crudo) de los eventos del recurso
    en la ventana. El denominador T_A es el tiempo total que el recurso está disponible
    según su calendario laboral entre tau_min y tau_max de la ventana.
    """

    eventos_recurso = ventana[ventana['org:resource'] == resource]

    if eventos_recurso.empty:
        return 0.0

    tiempo_procesamiento = eventos_recurso['TP'].sum()

    # Tiempo disponible del recurso en la ventana según su calendario laboral.
    # Reutilizamos _calcular_TPA aplicado al span completo de la ventana: la lógica es la misma que para un evento,
    # pero ahora "el evento" es la ventana entera, por lo que el resultado es el sumatorio de segundos laborables
    # del recurso entre tau_min y tau_max según su calendario.
    # Tau_min y tau_max se calculan a partir de la ventana, no de los eventos
    #tau_min = ventana['start_timestamp'].min()
    #tau_max = ventana['time:timestamp'].max()

    if DEBUG:
        logger.debug(f"Calculando tiempo disponible para el recurso {resource} entre {tau_min} y {tau_max}")
        logger.debug(f"Cantidad de eventos del recurso en la ventana: {len(eventos_recurso)}")

    tiempo_disponible = resource_profiles._calcular_TPA(tau_min, tau_max, resource, calendario)

    utilizacion = tiempo_procesamiento / tiempo_disponible if tiempo_disponible > 0 else 0.0

    if DEBUG:
        logger.debug(f"Recurso {resource}: T_P={tiempo_procesamiento:.2f}s, T_A={tiempo_disponible:.2f}s, utilización={utilizacion:.4f}")

    return utilizacion

def resource_utilization(ventana: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> dict[str, float]:
    """
    Calcula el resource utilization index para todos los recursos de la ventana.

    Mide el porcentaje de tiempo que cada recurso pasa trabajando en el proceso respecto al
    tiempo total que el recurso está disponible en el intervalo de la ventana.

    El calendario se obtiene en cada ventana llamando a `_obtener_calendario` (mismo
    mecanismo que la perspectiva de productividad) para que las actualizaciones publicadas
    por la perspectiva productora `calendarios` se vean inmediatamente, sin esperar al
    siguiente redescubrimiento de modelo.

    Args:
        ventana: DataFrame con la ventana de datos (columna TP ya calculada).
        modelo: No usado (utilization no tiene modelo propio).
        config: Diccionario con la configuración (incluye estado de dependencias).
        metrica: Nombre de la métrica a calcular.

    Returns:
        Un diccionario con el resource utilization index para cada recurso.
    """

    DEBUG = config.get('debug', False)

    # Adaptar la ventana para que tenga el formato esperado por _obtener_calendario
    ventana = resource_profiles._preparar_log_calendarios(ventana, config)

    calendario = resource_profiles._obtener_calendario(ventana, config)

    recursos = ventana['org:resource'].unique()

    resource_utilization = {}

    for resource in recursos:

        utilizacion = _resource_utilization_per_resource(resource, ventana, calendario, config['inicio'], config['fin'], DEBUG)

        if DEBUG:
            logger.debug(f"Recurso: {resource}, Utilización: {utilizacion:.4f}")

        resource_utilization[resource] = utilizacion

    return resource_utilization


def comparar_pertenencia_a_distribucion(ventana: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> dict[str, bool]:
    """
    Compara las productividades de la ventana contra la distribución de referencia
    ajustada por par (recurso, tarea). Devuelve, por cada par presente en la ventana,
    un booleano que indica si la ventana es "candidata" a drift para ese par
    (más de la mitad de sus productividades son anómalas según un p-valor bilateral < 0.05).

    La ventana llega ya transformada: una fila por par (recurso, tarea) con la lista
    de productividades de la ventana en la columna 'resource_productivity'.
    """

    resultado = {}

    distribuciones_recurso_tarea = modelo['modelo']

    for _, fila in ventana.iterrows():

        recurso         = fila['org:resource']
        tarea           = fila['concept:name']
        productividades = fila['resource_productivity']

        # Pares nuevos (recurso o tarea sin distribución de referencia) se omiten por ahora.
        if (recurso, tarea) not in distribuciones_recurso_tarea:
            logger.debug(f"Sin distribución de referencia para el par ({recurso}, {tarea}); se omite.")
            continue

        familia_guardada     = distribuciones_recurso_tarea[(recurso, tarea)]['familia']
        parametros_guardados = distribuciones_recurso_tarea[(recurso, tarea)]['parametros']

        # Contador de anómalos por par; se reinicia en cada par.
        anomalos       = 0
        posible_cambio = False

        for p in productividades:

            # p-valor bilateral: 2 * min(F(p), 1 - F(p)). Detecta desplazamientos
            # tanto por cola izquierda como por cola derecha.
            probabilidad_acumulada = familia_guardada.cdf(p, *parametros_guardados)
            p_valor                = 2 * min(probabilidad_acumulada, 1 - probabilidad_acumulada)

            if p_valor < 0.05:

                logger.debug(
                    f"Evento con productividad {p:.4f} para recurso {recurso} y tarea {tarea} "
                    f"tiene p-valor {p_valor:.4f}, podría no pertenecer a la distribución ajustada."
                )

                anomalos += 1

                # Early exit cuando ya hay mayoría de anómalos en la lista del par.
                if anomalos > len(productividades) / 2:
                    logger.warning(
                        f"Más de la mitad de las productividades para el par (recurso={recurso}, tarea={tarea}) "
                        f"son anómalas según la distribución ajustada."
                    )
                    posible_cambio = True
                    break

        resultado[f"({recurso}, {tarea})"] = posible_cambio

    return resultado

def comparar_distribuciones(ventana: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> dict[tuple, float]:
    """
    Mide, por par (recurso, tarea), la distancia Wasserstein entre la distribución de
    referencia (la del modelo, congelada hasta el próximo redescubrimiento) y la
    distribución de la ventana actual.

    La distribución de la ventana se obtiene re-ajustando LA MISMA familia que la
    referencia sobre las productividades del par en la ventana (no se re-ejecuta la
    selección por AIC: cambiar de familia entre ventanas metería ruido en la regresión
    posterior).

    Devuelve {(recurso, tarea): distancia}. Las claves se entregan como tupla (no
    string) para que el orquestador pueda pasarlas directamente al modelo en un
    redescubrimiento selectivo, sin necesidad de parseo. Los pares sin distribución
    de referencia, o con muy pocas muestras en la ventana para un re-ajuste estable,
    se omiten: el orquestador interpreta esa ausencia como "racha congelada" y no
    actualiza su historial.

    La ventana llega ya transformada: una fila por par (recurso, tarea) con la lista de
    productividades en la columna 'resource_productivity'.
    """

    # Mínimo de muestras por par/ventana para re-ajustar la familia de forma estable. Con
    # menos puntos el fit es inestable y la distancia se vuelve ruido.
    _MIN_MUESTRAS_DISTANCIA = 5

    DEBUG = config.get('debug', False)

    resultado: dict[tuple, float] = {}

    distribuciones_recurso_tarea = modelo['modelo']

    recursos = ventana['org:resource'].unique()
    tareas = ventana['concept:name'].unique()

    combinaciones = list(itertools.product(recursos, tareas))

    for recurso, tarea in combinaciones:

        # Pares sin distribución de referencia (recurso o tarea nuevos): se omiten.
        if (recurso, tarea) not in distribuciones_recurso_tarea:
            logger.info(f"Sin distribución de referencia para el par ({recurso}, {tarea}); se omite.")
            continue

        # La referencia puede ser None si en su día ninguna familia se ajustó al par.
        info_referencia = distribuciones_recurso_tarea[(recurso, tarea)]
        if info_referencia is None:
            logger.info(f"La referencia del par ({recurso}, {tarea}) es None; se omite.")
            continue

        familia_referencia = info_referencia['familia']
        parametros_referencia = info_referencia['parametros']

        # La ventana transformada tiene una fila por par con la lista de productividades.
        subventana = ventana[(ventana['org:resource'] == recurso) & (ventana['concept:name'] == tarea)]
        datos = np.asarray(subventana['resource_productivity'].iloc[0], dtype=float)

        # Evitar ajustes con pocas muestras en la ventana.
        if len(datos) < _MIN_MUESTRAS_DISTANCIA:
            logger.info(
                f"Par ({recurso}, {tarea}) con {len(datos)} muestras (< {_MIN_MUESTRAS_DISTANCIA}); "
                f"se omite del cálculo de distancia."
            )
            continue

        # Winsorizar para limitar el peso de los outliers en el re-ajuste.
        datos = resource_profiles._winsorizar(datos)

        # Re-ajustar la MISMA familia sobre la ventana. Para familias de soporte
        # estrictamente positivo se fija floc=0.
        try:
            if familia_referencia.name in resource_profiles._FAMILIAS_SOPORTE_POSITIVO:
                parametros_nueva_ventana = familia_referencia.fit(datos, floc=0)
            else:
                parametros_nueva_ventana = familia_referencia.fit(datos)
        except Exception as e:
            logger.info(
                f"No se pudo re-ajustar la familia '{familia_referencia.name}' para el par "
                f"({recurso}, {tarea}) ({e.__class__.__name__}); se omite."
            )
            continue

        # Distancia Wasserstein
        _EPSILON_CUANTIL = 1e-3
        _NUM_PUNTOS_CUANTIL = 1000

        # Definir cuantiles para evaluar la distancia
        cuantiles      = np.linspace(_EPSILON_CUANTIL, 1 - _EPSILON_CUANTIL, _NUM_PUNTOS_CUANTIL)
    
        # Generamos dos curvas comparables con la misma cantidad de puntos usando la Función de Puntos Porcentuales (PPF)
        ppf_referencia = familia_referencia.ppf(cuantiles, *parametros_referencia)
        ppf_ventana    = familia_referencia.ppf(cuantiles, *parametros_nueva_ventana)
    
        # Distancia
        distancia_wasserstein = st.wasserstein_distance(ppf_referencia, ppf_ventana)
        
        if DEBUG:
            logger.debug(f"Par ({recurso}, {tarea}): distancia Wasserstein = {distancia_wasserstein:.6f}")

        resultado[(recurso, tarea)] = distancia_wasserstein

    return resultado

        


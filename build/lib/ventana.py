import pandas as pd
import numpy as np
import pm4py 
import argparse
import logging
import sklearn

from collections import deque
from scipy.stats import linregress

from pm4py import Marking, PetriNet
from typing import Optional, Tuple, Dict, Any
from datetime import datetime
from datetime import date
from prefect import task
from pathlib import Path

from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import GridSearchCV, LeaveOneOut, ParameterGrid, RandomizedSearchCV, TimeSeriesSplit
from .logging_config import get_logger
from pm4py.algo.simulation.playout.petri_net import algorithm as simulator
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import VarianceThreshold

logger = get_logger(__name__)

MODOS_FILTRADO_TEMPORAL = {
    "eventos"  : "events",
    "trazas_contenidas": "traces_contained",
    "trazas_intersecadas" : "traces_intersecting"
}

# --- TAREAS DE AVANCE Y EXTRACCIÓN DE VENTANA ---

#@task
def comprobar_condicion_finalizacion(log: pd.DataFrame, ventana_actual: pd.DataFrame, verbose: bool = False) -> bool:
    """
    Comprueba si el final de la ventana actual coincide con el final del log completo.

    Args:
        log: DataFrame con el log de eventos completo.
        ventana_actual: DataFrame con la ventana de eventos actual.

    Returns:
        True si se ha alcanzado el final del log, False en caso contrario.
    """
    # Comprueba si la ventana está vacía
    if ventana_actual.empty or log.empty:
        return False
    
    # Comprueba si el último evento del segmento coincide con el último evento del log total.
    ultimo_elemento_ventana = ventana_actual.iloc[-1]
    ultimo_elemento_log = log.iloc[-1]

    if verbose:
        logger.debug(f"Ultimo elemento ventana (concept:name) {ultimo_elemento_ventana['concept:name']}")
        logger.debug(f"Ultimo elemento ventana (case:concept:name) {ultimo_elemento_ventana['case:concept:name']}")

        logger.debug(f"Ultimo elemento log (concept:name) {ultimo_elemento_log['concept:name']}")
        logger.debug(f"Ultimo elemento log (case:concept:name) {ultimo_elemento_log['case:concept:name']}")
        
    if (ultimo_elemento_ventana['concept:name'].strip() == ultimo_elemento_log['concept:name'].strip()) and (ultimo_elemento_ventana['case:concept:name'].strip() == ultimo_elemento_log['case:concept:name'].strip()):
        return True
    else:
        return False
    
def extraccion_ventana_temporal(log: pd.DataFrame, config: dict, parametros_ventana: dict) -> Tuple[pd.DataFrame, Dict[str, datetime]]:
    """
    Filtra el log de eventos por un rango de tiempo, creando una ventana temporal.
    Si no se proporciona una fecha inicial, utiliza la fecha mínima del log.

    Args:
        log: DataFrame con el log de eventos.
        parametros: Diccionario con los parámetros de configuración, incluyendo 'tamano_ventana'.

    Returns:
        Una tupla con el log filtrado (pd.DataFrame) y un diccionario de estado temporal {'inicio': datetime, 'fin' : datetime}.
    """
    
    DEBUG = config['debug']

    # Obtener el tamaño de ventana de los parámetros de configuración.
    tamano_ventana = parametros_ventana['tamano_ventana']

    if DEBUG:
        logger.debug(f"Iniciando filtrado con ventana de: {tamano_ventana}")

    # Comprobar si se necesita definir la fecha inicial.
    if parametros_ventana['fecha_inicial'] is None:
        fecha_inicial = pd.Timestamp(log['time:timestamp'].min())
    else:
        # Convertir a pd.Timestamp para asegurar compatibilidad con pd.Timedelta
        fecha_inicial = pd.Timestamp(parametros_ventana['fecha_inicial'])

    # Calcular la fecha final sumando el tamaño de la ventana.
    fecha_final = fecha_inicial + pd.Timedelta(tamano_ventana)

    # Convertir las fechas a formato string para la función de PM4Py.
    # Se ajustan las horas manualmente para cubrir todo el día.
    fecha_inicial_str = fecha_inicial.strftime('%Y-%m-%d %H:%M:%S')
    fecha_final_str = fecha_final.strftime('%Y-%m-%d %H:%M:%S')

    # Asegurar que la columna de timestamps está en formato datetime (no string)
    log['time:timestamp'] = pd.to_datetime(log['time:timestamp'], format='mixed')

    # Filtrar el log usando el modo especificado en los parámetros.
    log_filtrado = pm4py.filter_time_range(log, fecha_inicial_str, fecha_final_str, mode=MODOS_FILTRADO_TEMPORAL[parametros_ventana['modo_filtrado']])

    if DEBUG:
        logger.debug(f"Log filtrado desde {fecha_inicial_str} hasta {fecha_final_str}")
        logger.debug(f"Número de eventos obtenidos: {log_filtrado.shape[0]}")
        logger.debug(f"Primeras filas del log filtrado:")
        logger.debug(f"\n{log_filtrado.head()}")
        # Guarda el log filtrado en un archivo CSV para depuración.
        # Reemplaza colones en el nombre del archivo (invalidos en Windows)
        fecha_inicial_filename = fecha_inicial_str.replace(':', '-')
        fecha_final_filename = fecha_final_str.replace(':', '-')
        log_filtrado.to_csv(f"./logs/{fecha_inicial_filename}_{fecha_final_filename}.csv")

    return log_filtrado, {'inicio': fecha_inicial, 'fin' : fecha_final, 'traza_nueva': True}

def extraccion_ventana_eventos(log: pd.DataFrame, config: dict, parametros_ventana: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Filtra el log por un número específico de eventos.

    Args:
        log: DataFrame con el log de eventos.
        parametros: Diccionario con los parámetros de configuración, incluyendo 'tamano_ventana'.

    Returns:
        Una tupla con el log filtrado (pd.DataFrame) y un diccionario de estado {'inicio': Any, 'fin' : Any} (índices).
    """

    DEBUG = config['debug']

    # Obtener el tamaño de ventana a partir de los parámetros.
    tamano_ventana = parametros_ventana['tamano_ventana']

    if DEBUG:
        logger.debug(f"Iniciando filtrado con una ventana de {tamano_ventana} eventos")

    # Determinar el índice inicial, usando el primer evento si no está definido.
    if parametros_ventana['primer_evento'] is None:
        indice_inicial = log.iloc[0]
    else:
        indice_inicial = parametros_ventana['primer_evento']

    # Calcular el índice final basado en el tamaño de la ventana de eventos.
    indice_final = indice_inicial + tamano_ventana

    # Se ordena el log por marca de tiempo para asegurar consistencia del índice.
    log_ordenado = log.sort_values(by='time:timestamp')

    # Aplicar el filtrado de índices al log ordenado.
    log_filtrado = log_ordenado.iloc[indice_inicial:indice_final].copy()

    if DEBUG:
        logger.debug(f"Log filtrado desde {indice_inicial} hasta {indice_final}")
        logger.debug(f"Número de eventos obtenidos: {log_filtrado.shape[0]}")
        logger.debug(f"Primeras filas del log filtrado:")
        logger.debug(f"\n{log_filtrado.head()}")
        # Guarda el log filtrado en un archivo CSV para depuración.
        log_filtrado.to_csv(f"./logs/{indice_inicial}_{indice_final}.csv")

    return log_filtrado, {'inicio': indice_inicial, 'fin' : indice_final, 'traza_nueva': True}


def extraccion_ventana_trazas(log: pd.DataFrame, config: dict, parametros_ventana: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Filtra el log extrayendo X trazas completas consecutivas.

    Args:
        log: DataFrame con el log de eventos.
        config: Diccionario de configuración global (incluye 'debug').
        parametros_ventana: Diccionario con los parámetros de la ventana:
            - 'tamano_ventana': número de trazas a extraer.
            - 'primera_traza': índice de inicio en la lista de trazas únicas (None = desde el principio).

    Returns:
        Una tupla con el log filtrado (pd.DataFrame) y un diccionario de estado
        {'inicio': int, 'fin': int, 'traza_nueva': bool} con los índices de traza (sobre la lista ordenada de trazas únicas).
    """

    DEBUG = config['debug']

    tamano_ventana = parametros_ventana['tamano_ventana']

    if DEBUG:
        logger.debug(f"Iniciando filtrado con una ventana de {tamano_ventana} trazas")

    # Obtener lista ordenada de trazas únicas según su primer evento en el tiempo.
    trazas_ordenadas = (
        log.sort_values('time:timestamp')
        .groupby('case:concept:name', sort=False)['time:timestamp']
        .min()
        .sort_values()
        .index
        .tolist()
    )

    # Determinar el índice inicial.
    if parametros_ventana['primera_traza'] is None:
        indice_inicial = 0
    else:
        indice_inicial = parametros_ventana['primera_traza']

    indice_final = indice_inicial + tamano_ventana

    # Seleccionar las trazas correspondientes a la ventana.
    trazas_ventana = trazas_ordenadas[indice_inicial:indice_final]

    # Filtrar todos los eventos que pertenezcan a esas trazas.
    log_filtrado = log[log['case:concept:name'].isin(trazas_ventana)].copy()

    if DEBUG:
        logger.debug(f"Ventana de trazas [{indice_inicial}, {indice_final})")
        logger.debug(f"Trazas seleccionadas: {trazas_ventana}")
        logger.debug(f"Número de eventos obtenidos: {log_filtrado.shape[0]}")
        logger.debug(f"\n{log_filtrado.head()}")
        log_filtrado.to_csv(f"./logs/trazas_{indice_inicial}_{indice_final}.csv")

    return log_filtrado, {'inicio': indice_inicial, 'fin': indice_final, 'traza_nueva': True}

#@task(name="Extracción ventana", retries=1, retry_delay_seconds=5)
def extraccion_ventana(log: pd.DataFrame, config: dict, parametros_ventana) -> pd.DataFrame:
    """
    Realiza un filtrado del log de eventos, delegando en funciones específicas
    según el tipo de ventana configurado (temporal o por eventos).

    Args:
        log: DataFrame con el log de eventos.
        parametros: Diccionario con los parámetros de configuración de la ventana.

    Returns:
        El resultado del filtrado, que es un DataFrame filtrado o el resultado de la función delegada.
    """

    # Delegar la ejecución al filtro temporal o por eventos.
    if parametros_ventana['tipo'] == 'temporal':
       
       return extraccion_ventana_temporal(log, config, parametros_ventana)
    
    elif parametros_ventana['tipo'] == 'eventos':

        return extraccion_ventana_eventos(log, config, parametros_ventana)

    elif parametros_ventana['tipo'] == 'trazas': 

        return extraccion_ventana_trazas(log, config, parametros_ventana)
    
def filtrar_trazas_completas(log: pd.DataFrame, parametros: dict):
    """
    """

    DEBUG = parametros['debug']

    actividad_inicio = parametros['primera_tarea']
    actividad_fin = parametros['ultima_tarea']
    
    
    casos_con_inicio = log[log['concept:name'] == actividad_inicio]['case:concept:name'].unique()
    
    casos_con_fin = log[log['concept:name'] == actividad_fin]['case:concept:name'].unique()
    
    casos_completos = set(casos_con_inicio).intersection(set(casos_con_fin))
    
    log_filtrado = log[log['case:concept:name'].isin(casos_completos)].copy()
    """
    if DEBUG:
        logger.info(f"Log filtrado:")
        for i in range(len(log_filtrado)):
            logger.info(f"  {log_filtrado.iloc[i]['case:concept:name']} - {log_filtrado.iloc[i]['concept:name']} - {log_filtrado.iloc[i]['time:timestamp']}")
    """ 
    return log_filtrado
    
def entro_traza_nueva(ventana_nueva: pd.DataFrame, ventana_anterior: pd.DataFrame, parametros_globales: dict) -> bool:

    ventana_anterior_trazas_completas = filtrar_trazas_completas(ventana_anterior, parametros_globales)
    ventana_nueva_trazas_completas = filtrar_trazas_completas(ventana_nueva, parametros_globales)

    trazas_anteriores = set(ventana_anterior_trazas_completas['case:concept:name'].unique())
    trazas_nuevas = set(ventana_nueva_trazas_completas['case:concept:name'].unique())

    # Si hay alguna traza nueva que no estaba en la ventana anterior, entonces entró una traza nueva.
    if not trazas_nuevas.issubset(trazas_anteriores):
        return True
    else:
        return False

def avanzar_ventana_temporal(log: pd.DataFrame, parametros_globales: dict, parametros_ventana: dict, estado_temporal: dict) -> Tuple[pd.DataFrame, dict]:
    """
    Avanza la ventana temporal del log de eventos aplicando el salto especificado a las fechas de corte.

    Args:
        log: DataFrame con el log de eventos.
        parametros_globales: Diccionario con los parámetros globales de configuración.
        parametros_ventana: Diccionario con los parámetros de configuración de la ventana.
        estado_temporal: Diccionario con el estado actual de la ventana temporal.

    Returns:
        Una tupla con el log filtrado (pd.DataFrame) y un diccionario con la nueva fecha inicial y final.
    """

    DEBUG = parametros_globales['debug']

    inicio = estado_temporal['inicio']
    fin = estado_temporal['fin']

    salto_ventana = parametros_ventana['salto_ventana']
    tamano_ventana = parametros_ventana['tamano_ventana']

    fecha_inicial_str = inicio.strftime('%Y-%m-%d %H:%M:%S')
    fecha_final_str = fin.strftime('%Y-%m-%d %H:%M:%S')

    if DEBUG:
        logger.debug(f"Avanzando ventana temporal. Fecha inicial actual: {fecha_inicial_str}, Fecha final actual: {fecha_final_str}, Salto de ventana: {salto_ventana}")

    # Calcular la nueva fecha inicial aplicando el salto.
    nueva_fecha_inicial = inicio + pd.Timedelta(salto_ventana)

    ultima_fecha_log = log['time:timestamp'].max()

    if nueva_fecha_inicial > ultima_fecha_log:
        nueva_fecha_inicial = ultima_fecha_log

    # Calcular la nueva fecha final aplicando el tamaño de ventana.
    nueva_fecha_final = nueva_fecha_inicial + pd.Timedelta(tamano_ventana)

    # Convertir las fechas a formato string
    fecha_inicial_str = nueva_fecha_inicial.strftime('%Y-%m-%d %H:%M:%S')
    fecha_final_str = nueva_fecha_final.strftime('%Y-%m-%d %H:%M:%S')

    if DEBUG:
        logger.debug(f"Nueva fecha inicial: {nueva_fecha_inicial}, Nueva fecha final: {nueva_fecha_final}")

    # Re-filtrar el log con el nuevo rango temporal.
    log_filtrado = pm4py.filter_time_range(log, fecha_inicial_str, fecha_final_str, mode=MODOS_FILTRADO_TEMPORAL[parametros_ventana['modo_filtrado']])

    return log_filtrado, {'inicio' : nueva_fecha_inicial, 'fin': nueva_fecha_final} 

def avanzar_ventana_eventos(log: pd.DataFrame, parametros_globales: dict, parametros_ventana: dict, estado_temporal: dict) -> Tuple[pd.DataFrame, Any, Any]:
    """
    Avanza la ventana basada en el número de eventos aplicando el salto especificado a los índices de corte.

    Args:
        log: DataFrame con el log de eventos.
        parametros: Diccionario con la configuración de la ventana (indice_inicial, indice_final, salto_ventana).

    Returns:
        Una tupla con el log filtrado (pd.DataFrame), el índice inicial anterior y el índice final anterior.
    """

    DEBUG = parametros_globales['debug']

    inicio = estado_temporal['inicio']
    fin = estado_temporal['fin']

    salto_ventana = parametros_ventana['salto_ventana']

    if DEBUG:
        logger.debug(f"Avanzando ventana por eventos. Indice inicial actual: {inicio}, Indice final actual: {fin}, Salto de ventana: {salto_ventana}")

    # Calcular el nuevo índice inicial aplicando el salto.
    nuevo_indice_inicial = inicio + salto_ventana
    nuevo_indice_final = fin + salto_ventana
    total_eventos = len(log)

    # Clampear al final del log
    nuevo_indice_inicial = min(nuevo_indice_inicial, total_eventos - 1)
    nuevo_indice_final = min(nuevo_indice_final, total_eventos)

    if DEBUG:
        logger.debug(f"Nuevo indice inicial: {nuevo_indice_inicial}, Nuevo indice final: {nuevo_indice_final}")

    # Se ordena el log
    log_ordenado = log.sort_values(by='time:timestamp')

    # Re-filtrar el log usando los nuevos índices.
    log_filtrado = log_ordenado.iloc[nuevo_indice_inicial:nuevo_indice_final].copy()
    
    return log_filtrado, {'inicio' : nuevo_indice_inicial, 'fin': nuevo_indice_final} 

def avanzar_ventana_trazas(log: pd.DataFrame, parametros_globales: dict, parametros_ventana: dict, estado_temporal: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Avanza la ventana de trazas desplazando el índice de inicio según el salto configurado.

    Args:
        log: DataFrame con el log de eventos.
        parametros_globales: Diccionario de configuración global (incluye 'debug').
        parametros_ventana: Diccionario con los parámetros de la ventana:
            - 'tamano_ventana': número de trazas en la ventana.
            - 'salto_ventana': número de trazas a avanzar en cada paso.
        estado_temporal: Diccionario con el estado actual {'inicio': int, 'fin': int}.

    Returns:
        Una tupla con el log filtrado (pd.DataFrame) y el nuevo estado
        {'inicio': int, 'fin': int}.
    """

    DEBUG = parametros_globales['debug']

    inicio = estado_temporal['inicio']
    fin = estado_temporal['fin']
    salto_ventana = parametros_ventana['salto_ventana']
    tamano_ventana = parametros_ventana['tamano_ventana']

    if DEBUG:
        logger.debug(f"Avanzando ventana por trazas. Índice inicial: {inicio}, Índice final: {fin}, Salto: {salto_ventana}")

    # Reconstruir lista ordenada de trazas (igual que en extraccion).
    trazas_ordenadas = (
        log.sort_values('time:timestamp')
        .groupby('case:concept:name', sort=False)['time:timestamp']
        .min()
        .sort_values()
        .index
        .tolist()
    )

    total_trazas = len(trazas_ordenadas)

    # Calcular nuevos índices y clampear al límite del log.
    nuevo_indice_inicial = min(inicio + salto_ventana, total_trazas - 1)
    nuevo_indice_final = min(nuevo_indice_inicial + tamano_ventana, total_trazas)

    trazas_ventana = trazas_ordenadas[nuevo_indice_inicial:nuevo_indice_final]

    log_filtrado = log[log['case:concept:name'].isin(trazas_ventana)].copy()

    if DEBUG:
        logger.debug(f"Nueva ventana de trazas [{nuevo_indice_inicial}, {nuevo_indice_final})")
        logger.debug(f"Trazas seleccionadas: {trazas_ventana}")
        logger.debug(f"Número de eventos obtenidos: {log_filtrado.shape[0]}")

    return log_filtrado, {'inicio': nuevo_indice_inicial, 'fin': nuevo_indice_final, 'traza_nueva': True}

#@task(name="Avanzar ventana temporal", retries=3, retry_delay_seconds=5)
def avanzar_ventana(log: pd.DataFrame, ventana_anterior: pd.DataFrame, parametros_globales: dict, parametros_ventana: dict, estado_temporal: dict) -> Tuple[pd.DataFrame, Any, Any]:
    """
    Avanza la ventana del log según el tipo configurado ('temporal' o 'eventos'), delegando la operación.

    Args:
        log: DataFrame con el log de eventos.
        ventana: DataFrame con la ventana actual.
        parametros_globales: Diccionario con la configuración global.
        parametros_ventana: Diccionario con la configuración de la ventana, incluyendo 'ventana' y 'salto_ventana'.

    Returns:
        Una tupla con el log segmentado (pd.DataFrame) y el estado de la ventana (inicio, final).
    """

    # Delegar la operación de avance de ventana.
    if parametros_ventana['tipo'] == 'temporal':
       
        ventana_nueva, estado_temporal = avanzar_ventana_temporal(log, parametros_globales, parametros_ventana, estado_temporal)
        estado_temporal.update({'traza_nueva': entro_traza_nueva(ventana_nueva, ventana_anterior, parametros_globales)})

    elif parametros_ventana['tipo'] == 'eventos':

        ventana_nueva, estado_temporal = avanzar_ventana_eventos(log, parametros_globales, parametros_ventana, estado_temporal)
        estado_temporal.update({'traza_nueva': entro_traza_nueva(ventana_nueva, ventana_anterior, parametros_globales)})

    elif parametros_ventana['tipo'] == 'trazas':

        ventana_nueva, estado_temporal = avanzar_ventana_trazas(log, parametros_globales, parametros_ventana, estado_temporal)
        estado_temporal.update({'traza_nueva': True})

    return ventana_nueva, estado_temporal

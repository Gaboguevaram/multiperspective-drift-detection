import pandas as pd
import numpy as np
import pm4py 
import argparse
import logging
import sklearn

from collections import deque
from scipy.stats import linregress, test

from pm4py import Marking, PetriNet
from typing import Optional, Tuple, Dict, Any
from datetime import datetime
from datetime import date
from prefect import task
from pathlib import Path

from sklearn.metrics import mean_absolute_error, mean_squared_error
from .perspectivas.arrival_rate import preprocesado_arrival_rate
from .perspectivas.service_rate import preprocesado_service_rate
from sklearn.model_selection import GridSearchCV, LeaveOneOut, ParameterGrid, RandomizedSearchCV, TimeSeriesSplit
from .logging_config import get_logger
from pm4py.algo.simulation.playout.petri_net import algorithm as simulator
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import VarianceThreshold
from pix_framework.discovery.resource_calendar_and_performance.crisp.resource_calendar import CalendarKPIInfoFactory
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

    DEBUG = config['debug']

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

    DEBUG = config['debug']

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
    DEBUG = config['debug']
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

    DEBUG = config['debug']
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

    df_recurso['weekday'] = df_recurso['start_time'].dt.dayofweek
    df_recurso['time'] = df_recurso['start_time'].dt.time
    
    total_eventos = len(df_recurso)
    eventos_cubiertos = 0

    if verbose:
        df_recurso[['weekday', 'time']].to_csv(f"./logs/df_recurso.csv", index=False)
        
    for index, evento in df_recurso.iterrows():
        
        if verbose:
            logger.debug(f"Evaluando evento: weekday={evento['weekday']}, time={evento['time']}")
        
        intervals = resource_calendar.intervals
        for i in intervals:

            week_day = False
            start_time = False
            end_time = False

            from_day_convertido = _convert_string_to_weekday(i.from_day)
            to_day_convertido = _convert_string_to_weekday(i.to_day)

            if verbose:
                logger.debug(f"Intervalo: {from_day_convertido} - {to_day_convertido} {i._start_time} - {i._end_time}")

            if evento['weekday'] == from_day_convertido or evento['weekday'] == to_day_convertido:
                week_day = True
                start_time_convertido = i._start_time.time()
                end_time_convertido = i._end_time.time()

            if week_day:

                if start_time_convertido <= evento['time']:
                    start_time = True
                    
                if end_time_convertido >= evento['time']:
                    end_time = True

            if verbose:
                logger.debug(f" => week_day: {week_day}, start_time: {start_time}, end_time: {end_time}")

            if week_day and start_time and end_time:
                logger.debug(f"Evento cubierto por el intervalo")
                eventos_cubiertos += 1
                break 

    if verbose:
        logger.debug(f"Total eventos para el recurso: {total_eventos}")
        logger.debug(f"Eventos cubiertos por el calendario: {eventos_cubiertos}")

    soporte_recurso = eventos_cubiertos / total_eventos if total_eventos > 0 else 0.0
    
    return soporte_recurso

def calcular_support(ventana: pd.DataFrame, modelo: dict, config: dict, metrica: str) -> float:
    """
    Calcula el soporte para el modelo de calendarios.

    Args:
        ventana: DataFrame con la ventana de datos.
        modelo: Diccionario con el modelo entrenado.
        config: Diccionario con la configuración.
        metrica: Nombre de la métrica a calcular.

    Returns:
        El valor de soporte como un float.
    """

    DEBUG = config['debug']

    resource_calendar = modelo['modelo']
    recursos = modelo['recursos']

    # FORMA A MANO
    support = []
    for r in recursos:
        # Obtener los eventos de la ventana que corresponden al recurso r
        df_r = ventana[ventana['resource'] == r].copy()
        # Obtener el calendario del recurso r
        for rc in resource_calendar:
            if rc.resource_name == r:
                resource_calendar_r = rc
                break
        # Calcular el soporte para el recurso r usando la función support_per_resource
        support_r = support_per_resource(df_r, resource_calendar_r, DEBUG)
        if DEBUG:
            logger.debug(f"Recurso: {r}, Soporte: {support_r:.4f}")
        support.append(support_r)

    if DEBUG:
        logger.debug(f"Soportes por recurso: {support}")

    support = np.mean(support)

    logger.info(f"Soporte calculado (forma manual): {support:.4f}")

    # FORMA USANDO PIX-FRAMEWORK
    """
    factory =  CalendarKPIInfoFactory()

    for row in ventana.itertuples():
        r = row.resource
        t = row.task
        dt = pd.Timestamp(row.start_time).to_pydatetime()
        factory.register_resource_timestamp(r, t, dt, is_joint=False)

    _, g_index, weekday = factory.split_datetime(pd.Timestamp("2026-04-24 08:00:00").to_pydatetime())

    support = factory.support(g_index, weekday)

    """

    return support


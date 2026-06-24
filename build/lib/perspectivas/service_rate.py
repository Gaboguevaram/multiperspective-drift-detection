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
from ..logging_config import get_logger
from pm4py.algo.simulation.playout.petri_net import algorithm as simulator
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import VarianceThreshold

logger = get_logger(__name__)

########################
# --- SERVICE-RATE --- #
########################

def filtrado_service_rate(log: pd.DataFrame, parametros: dict):
    """
    Devuelve un log filtrado que solo contiene los eventos correspondientes al fin de una traza.
    """
    DEBUG = parametros['debug']

    logger.info("Filtrando log para cálculo de service rate (solo eventos de fin de traza)")

    # Se usa la tarea final del proceso para filtrar
    log_filtrado = log[log['concept:name'] == parametros['ultima_tarea']].copy()
    
    log_filtrado = log.sort_values('time:timestamp')
    
    # Eliminar la zona horaria (por conveniencia en el cálculo de service rate)

    # Se convierte a datetime para asegurarnos de que es del tipo correcto
    log_filtrado['time:timestamp'] = pd.to_datetime(log_filtrado['time:timestamp'])

    # Se eliminan las zonas horarias para evitar problemas en el cálculo de diferencias de tiempo
    log_filtrado['time:timestamp'] = log_filtrado['time:timestamp'].dt.tz_localize(None)

    log_filtrado = log_filtrado[['case:concept:name', 'time:timestamp', 'trace_real_index']]    

    if DEBUG:
        log_filtrado.to_csv(f"./logs/service_rate_filtrado.csv", index=False)

    return log_filtrado

def transformacion_service_rate(log: pd.DataFrame, parametros: dict):
    """
    Calcula la tasa de servicio (service rate) agrupando eventos por período de tiempo.
    """

    DEBUG = parametros['debug']

    logger.info("Transformando log para cálculo de service rate")

    granularidad_service_rate = parametros['granularidad_service_rate']
    frecuencia_service_rate = parametros['frecuencia_service_rate']

    if DEBUG:
        logger.debug(f"Granularidad para service rate: {granularidad_service_rate}")
        logger.debug(f"Frecuencia para service rate: {frecuencia_service_rate}")

    fecha_inicial = log['time:timestamp'].min()
    fecha_inicial_str = fecha_inicial.strftime('%Y-%m-%d %H:%M:%S')

    fecha_final = fecha_inicial + pd.Timedelta(granularidad_service_rate)
    fecha_final_str = fecha_final.strftime('%Y-%m-%d %H:%M:%S')

    ultima_fecha_log = log['time:timestamp'].max()
    resultados = []
    iter = 0

    while fecha_final <= ultima_fecha_log:

        fecha_inicial = fecha_inicial + pd.Timedelta(frecuencia_service_rate)
        fecha_final = fecha_inicial + pd.Timedelta(granularidad_service_rate)
        
        fecha_inicial_str = fecha_inicial.strftime('%Y-%m-%d %H:%M:%S')
        fecha_final_str = fecha_final.strftime('%Y-%m-%d %H:%M:%S')
        iter += 1

        if DEBUG:
            logger.debug(f"Procesando ventana desde {fecha_inicial_str} hasta {fecha_final_str}")

        ventana_service_rate = log[(log['time:timestamp'] >= pd.Timestamp(fecha_inicial)) & (log['time:timestamp'] < pd.Timestamp(fecha_final))].copy()

        service_rate = ventana_service_rate.shape[0]

        #logger.debug(f"Service rate en esta ventana: {service_rate} eventos")
        
        # TODO: ver que hacer con case:conceptname y trace_real_index, ya que ahora cada fila representa una ventana de tiempo, no una traza individual.
        resultados.append({
            'time:timestamp': fecha_final,
            'case:concept:name': ventana_service_rate['case:concept:name'].iloc[-1] if not ventana_service_rate.empty else caso_ventana_previa,  # Caso de la última traza en la ventana (representativo)
            'trace_real_index': ventana_service_rate['trace_real_index'].iloc[-1] if not ventana_service_rate.empty else traza_ventana_previa,  # Trace_real_index de la última traza en la ventana
            'service_rate': service_rate
        })

        if service_rate > 0:
            caso_ventana_previa = ventana_service_rate['case:concept:name'].iloc[-1]
            traza_ventana_previa = ventana_service_rate['trace_real_index'].iloc[-1]

        if DEBUG:
            ventana_service_rate.to_csv(f"./logs/service_rate_ventana_{iter}.csv", index=False)

    log_transformado = pd.DataFrame(resultados)

    # Extraemos componentes temporales para enriquecer el modelo
    log_transformado['year'] = log_transformado['time:timestamp'].dt.year
    log_transformado['month'] = log_transformado['time:timestamp'].dt.month
    log_transformado['day'] = log_transformado['time:timestamp'].dt.day
    log_transformado['hour'] = log_transformado['time:timestamp'].dt.hour
    log_transformado['minute'] = log_transformado['time:timestamp'].dt.minute
    log_transformado['second'] = log_transformado['time:timestamp'].dt.second
    log_transformado['week_day'] = log_transformado['time:timestamp'].dt.dayofweek
    log_transformado['es_fin_de_semana'] = log_transformado['week_day'] >= 5

    log_transformado = log_transformado.sort_values('time:timestamp').reset_index(drop=True)

    if DEBUG:
        log_transformado.to_csv(f"./logs/service_rate_transformado.csv", index=False)

    # Eliminamos la columna de timestamp original, ya que ahora tenemos las componentes temporales separadas.
    #log_transformado = log_transformado.drop(columns=['time:timestamp'])

    return log_transformado

def _obtener_temporalidades_superiores(frecuencia: str) -> list[str]:

    temporalidades = ['second', 'min', 'hour', 'day', 'week_day', 'week', 'month']

    temporalidad = frecuencia.split(' ')[1]

    if temporalidad not in temporalidades:
        raise ValueError(f"Temporalidad desconocida: {temporalidad}. Las opciones válidas son: {temporalidades}")
    
    indice = temporalidades.index(temporalidad)

    return temporalidades[indice+1:]

def _calcular_periodos_frecuencias(frecuencia_str: str, temporalidades: list[str]) -> dict[str, int]:

    frecuencia_base = int(frecuencia_str.split(' ')[0])
    temporalidad = frecuencia_str.split(' ')[1]

    if temporalidad not in temporalidades:
        raise ValueError(f"Temporalidad desconocida: {temporalidad}. Las opciones válidas son: {temporalidades}")
    
    # Convertir a segundos para facilitar el cálculo de periodos
    if temporalidad == 'second':
        pass
    elif temporalidad == 'min':
        frecuencia_base *= 60
    elif temporalidad == 'hour':
        frecuencia_base *= 3600
    elif temporalidad == 'day':
        frecuencia_base *= 3600 * 24
    elif temporalidad == 'week':
        frecuencia_base *= 3600 * 24 * 7
    elif temporalidad == 'month':
        frecuencia_base *= 3600 * 24 * 7 * 4 
    
    # Cálculo de frecuencias
    frecuencias = {}
    for temporalidad in temporalidades:
        if temporalidad == 'second':
            frecuencias['second'] = frecuencia_base
        elif temporalidad == 'min':
            frecuencias['min'] = frecuencia_base / 60
        elif temporalidad == 'hour':
            frecuencias['hour'] = frecuencia_base / 3600
        elif temporalidad == 'day':
            frecuencias['day'] = frecuencia_base / (3600 * 24)
        elif temporalidad == 'week':
            frecuencias['week'] = frecuencia_base / (3600 * 24 * 7)
        elif temporalidad == 'month':
            frecuencias['month'] = frecuencia_base / (3600 * 24 * 7 * 4)


    periodos = {}
    # Cálculo de períodos
    for temporalidad in temporalidades:
        if temporalidad == 'min':
            periodos['min'] = float(60 / frecuencia_base)
        elif temporalidad == 'hour':
            periodos['hour'] = float(60 * periodos.get('min'))  # 60 minutos en una hora, dividido por la frecuencia de service rate (ej. 15 min -> 4 periodos de 15 min en una hora)
        elif temporalidad == 'day':
            periodos['day'] = float(24 * periodos.get('hour'))
        elif temporalidad == 'week':
            periodos['week'] = float(7 * periodos.get('day'))
        elif temporalidad == 'month':
            periodos['month'] = float(12 * periodos.get('day'))

    # Convertir el período a entero
    periodos = {k: int(v) for k, v in periodos.items()}

    return periodos, frecuencias

def preprocesado_service_rate(log: pd.DataFrame, parametros: dict, verbose: bool, test: bool, features: list = None) -> pd.DataFrame:
    """
    Preprocesa el log para el cálculo del service rate. Esto incluye la eliminación de columnas irrelevantes y la selección de características con varianza no nula.
    """

    DEBUG = parametros['debug']

    # Columnas que no aportan información
    # El año no es relevante, pues no es algo estacional
    columnas_a_eliminar = ['case:concept:name', 'trace_real_index', 'year']

    log_preprocesado = log.copy()

    ##################################################
    # Codificar las variables temporales en períodos #
    ##################################################

    # Obtener las temporalidades superiores a la frecuencia actual
    frecuencia_service_rate = parametros['frecuencia_service_rate']

    temporalidades = ['second', 'min', 'hour', 'day', 'week', 'month']
    periodos, frecuencias = _calcular_periodos_frecuencias(frecuencia_service_rate, temporalidades)

    log_preprocesado[f'intervalo_minuto'] = log_preprocesado['second'] // frecuencias['second']
    log_preprocesado[f'intervalo_hora'] = log_preprocesado['minute'] // frecuencias['min'] + log_preprocesado['intervalo_minuto']
    log_preprocesado[f'intervalo_diario'] = log_preprocesado['hour'] // frecuencias['hour'] + log_preprocesado['intervalo_hora']
    log_preprocesado[f'intervalo_semanal'] = log_preprocesado['week_day'] // frecuencias['day'] + log_preprocesado['intervalo_diario']
    log_preprocesado[f'intervalo_mensual'] = log_preprocesado['month'] // frecuencias['week'] + log_preprocesado['intervalo_semanal']
  
    log_prueba = log_preprocesado.copy()
    log_prueba.drop(columns=['time:timestamp', 'case:concept:name', 'trace_real_index', 'service_rate', 'year', 'month', 'day', 'hour', 'minute', 'week_day', 'es_fin_de_semana' ,'second'], inplace=True)

    if DEBUG:
        log_prueba.to_csv(f"./logs/service_rate_preprocesado_intervalos.csv", index=False)

    ####################################################################
    # Aplicación de codificación ciclíca para las variables temporales #
    ####################################################################

    if verbose:
        logger.info("Períodos calculados para codificación cíclica:")
        for temporalidad, periodo in periodos.items():
            logger.info(f"  {temporalidad}: {periodo}")

    for tupla in periodos.items():

        temporalidad, periodo = tupla
        
        if periodo <= 1:
            logger.warning(f"El período calculado para la temporalidad {temporalidad} es {periodo}, lo cual no es válido para la codificación cíclica. Se omitirá esta temporalidad.")
            continue

        if temporalidad == 'min':
            log_preprocesado['Min_sin'] = np.sin(log_preprocesado['intervalo_minuto'] * (2 * np.pi / periodo))
            log_preprocesado['Min_cos'] = np.cos(log_preprocesado['intervalo_minuto'] * (2 * np.pi / periodo))
        if temporalidad == 'hour':
            log_preprocesado['Hour_sin'] = np.sin(log_preprocesado['intervalo_hora'] * (2 * np.pi / periodo))
            log_preprocesado['Hour_cos'] = np.cos(log_preprocesado['intervalo_hora'] * (2 * np.pi / periodo))
        elif temporalidad == 'day':
            log_preprocesado['Day_sin'] = np.sin(log_preprocesado['intervalo_diario'] * (2 * np.pi / periodo))
            log_preprocesado['Day_cos'] = np.cos(log_preprocesado['intervalo_diario'] * (2 * np.pi / periodo))
        elif temporalidad == 'week':
            log_preprocesado['Week_sin'] = np.sin(log_preprocesado['intervalo_semanal'] * (2 * np.pi / periodo))
            log_preprocesado['Week_cos'] = np.cos(log_preprocesado['intervalo_semanal'] * (2 * np.pi / periodo))
        elif temporalidad == 'month':
            log_preprocesado['Month_sin'] = np.sin(log_preprocesado['intervalo_mensual'] * (2 * np.pi / periodo))
            log_preprocesado['Month_cos'] = np.cos(log_preprocesado['intervalo_mensual'] * (2 * np.pi / periodo))

    columnas_a_eliminar += ['time:timestamp', 'second', 'minute', 'day', 'hour','month','week_day', 'intervalo_minuto', 'intervalo_hora', 'intervalo_diario', 'intervalo_semanal', 'intervalo_mensual']

    # Eliminar los columnas de meses
    columnas_a_eliminar += ['Month_sin', 'Month_cos']

    ###########################
    # Adición de lag features #
    ###########################

    # Agregar features de lag de las 3 filas anteriores
    for i in range(1, 4):
        log_preprocesado[f'service_rate_lag_{i}'] = log_preprocesado['service_rate'].shift(i)

    if not test:
        # Se eliminan las filas con valores NaN (son solo 3)
        log_preprocesado.dropna(inplace=True)

    # Agregar una feature por temporalidad superior
    for temporalidad in temporalidades[1:]:  # Empezamos desde la segunda, ya que la primera es 'second', que no tiene sentido como temporalidad superior
        if periodos[temporalidad] <= 1:
            logger.warning(f"El período calculado para la temporalidad {temporalidad} es {periodos[temporalidad]}, lo cual no es válido para la creación de features de lag. Se omitirá esta temporalidad.")
            continue
        log_preprocesado[f'service_rate_lag_{periodos[temporalidad]}'] = log_preprocesado['service_rate'].shift(periodos[temporalidad])

    # Rellenar los NanN
    # Interpolación lineal (muy útil en series de tiempo)
    #log_preprocesado.interpolate(method='linear', inplace=True)

    #if log_preprocesado.isna().any().any():
    #    logger.warning(f"Existen valores NaN en el DataFrame preprocesado")

    #######################################
    # Remover variables con varianza cero #
    #######################################

    # Eliminar las columnas originales que ya no son necesarias para el modelo
    log_preprocesado = log_preprocesado.drop(columns=columnas_a_eliminar, errors='ignore')

    """

    # Removemos las variables que tiene datos idénticos en todas las filas (varianza cero)
    selector = VarianceThreshold()

    variables_con_varianza = selector.fit_transform(log_preprocesado)
    
    log_preprocesado = pd.DataFrame(variables_con_varianza, columns=log_preprocesado.columns[selector.get_support()])

    """
    if DEBUG:
        log_preprocesado.to_csv(f"./logs/service_rate_preprocesado.csv", index=False)

    return log_preprocesado

def modelo_service_rate(log: pd.DataFrame, parametros: dict, random_state: int = 42) -> dict:
    """
    Entrena un modelo de regresión para predecir el service rate.
    """

    DEBUG = parametros['debug']

    logger.info("Descubriendo Modelo de Service Rate")

    # Entrenar el modelo usando los datos de service rate
    modelo = RandomForestRegressor(criterion="squared_error", random_state=random_state)

    log_preprocesado = preprocesado_service_rate(log, parametros)

    # Separar características y variable objetivo
    train_X = log_preprocesado.drop(columns=["service_rate"])
    train_Y = log_preprocesado["service_rate"]

    if DEBUG:
        logger.debug(f"Datos de entrenamiento (train_X): {train_X}")
        logger.debug(f"Datos de la variable objetivo de entrenamiento (train_Y): {train_Y}")

    # Grid de hiperparámetros a probar
    param_grid = {
        "max_depth": range(1, 11),
        "min_samples_split": range(2, 13),
        "min_samples_leaf": range(1, 11)
    }

    # Método de división TimeSeriesSplit para validación cruzada
    tscv = TimeSeriesSplit(n_splits=3)
    #tscv = TimeSeriesSplit(n_splits=len(log_preprocesado)-1)

    # Grid search
    grid = GridSearchCV(
        estimator=modelo,
        param_grid=param_grid,
        cv=tscv,
        scoring="neg_mean_absolute_error",
        n_jobs=1 
    )

    logger.info("Realizando Grid Search para optimización de hiperparámetros...")

    grid.fit(train_X, train_Y)

    logger.info(f"Mejores parámetros: {grid.best_params_}")
    logger.info(f"Mejor resultado: {abs(grid.best_score_):.4f} (MAE)")

    modelo = grid.best_estimator_

    return {'modelo': modelo, 'features': train_X.columns.tolist(), 'objective': 'service_rate'}
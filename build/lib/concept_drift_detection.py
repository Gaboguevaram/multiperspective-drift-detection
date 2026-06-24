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

# --- TAREAS DE DETECCIÓN DE CONCEPT DRIFT ---

def identificar_candidato_cambio(ventana_regresion: int, hist_resultados: list[float], hist_candidatos: list[bool], tipo_pendiente_racha: str = None, verbose: bool = False) -> Tuple[list[bool], Optional[str]]:
    """
    Identifica si existe una tendencia significativa (regresión lineal) en las últimas métricas para marcar un candidato a drift.

    Args:
        ventana_regresion: Número de puntos de datos recientes a considerar para la regresión.
        hist_resultados: Historial completo de los valores de la métrica (fitness o precision).
        hist_candidatos: Historial de indicadores booleanos de candidatez.
        tipo_pendiente_racha: Tipo de pendiente que inició la racha.
        verbose: Si es True, se imprimirán detalles adicionales para depuración.
    Returns:
        El historial actualizado de indicadores de candidatez.
        Posibles devoluciones:
            - Cambio detectado con pendiente negativa lista[-1] y 'negativa'
            - Cambio detectado con pendiente positiva lista[-1] y 'positiva'
            - Cambio detectado con pendiente neutra lista[-1] y 'neutra
    """
    es_candidato = False

    if len(hist_resultados) > ventana_regresion:

        y = hist_resultados[-ventana_regresion:]
        x = range(len(y))
        
        # Realizar regresión lineal sobre los últimos 'ventana_regresion' puntos.
        slope, intercept, r_value, p_value, std_err = linregress(x, y)

        if verbose:
            logger.debug(f"Últimos {ventana_regresion} valores para regresión: {y}")
            logger.debug(f"Ventana de regresión: {ventana_regresion}")
            logger.debug(f"Pendiente recta regresión: {slope}")
            logger.debug(f"P-valor: {p_value}")

        # Reglas:
        # Pendiente negativa (m<)
        m_neg = (slope < 0 and p_value < 0.05)
        # Pendiente positiva (m>)
        m_pos = (slope > 0 and p_value < 0.05)
        # Pendiente neutra
        m_zero = not m_neg and not m_pos

        if verbose:
            logger.debug(f"m_neg: {m_neg}, m_pos: {m_pos}, m_zero: {m_zero}")
            logger.debug(f"Tipo de pendiente de racha actual: {tipo_pendiente_racha}")

        # Candidato previo
        if len(hist_candidatos) > 0:
            prev_candidato = bool(hist_candidatos[-1])
        else:
            prev_candidato = False

        if m_neg or m_pos or (m_zero and prev_candidato):
            es_candidato = True

        # Cancelación de racha
        if m_neg and tipo_pendiente_racha == 'positiva':
            if verbose:
                logger.debug(f"   [!] Tipo de racha: {tipo_pendiente_racha}, pendiente actual no positiva -> RACHA ROTA")
            hist_candidatos.append(False)
            return hist_candidatos, None  # Racha rota, no hay candidato
        if m_pos and tipo_pendiente_racha == 'negativa':
            if verbose:
                logger.debug(f"   [!] Tipo de racha: {tipo_pendiente_racha}, pendiente actual no negativa -> RACHA ROTA")
            hist_candidatos.append(False)
            return hist_candidatos, None  # Racha rota, no hay candidato
        
        #TODO: Ahora mismo, una cancelación de racha no supone el inicio de una nueva
        # Solucionable en confirmar cambio??, guardar el tipo de pendiente que inicio la racha y q sea el mismo

        if es_candidato:
            logger.warning(f"   [!] Ventana marcada como CANDIDATA a drift (Pendiente: {slope:.4f}, p-value: {p_value:.4f})")
            razon = 'Pendiente negativa' if m_neg else 'Pendiente positiva' if m_pos else 'Pendiente neutra con candidato previo'
            logger.warning(f"Razón: {razon}")
            tipo_pendiente_racha = 'negativa' if m_neg else 'positiva' if m_pos else 'neutra'

    else:
        if verbose:
            logger.debug(f"No hay suficientes datos para identificar candidato a drift (tamaño historial: {len(hist_resultados)})")

    hist_candidatos.append(es_candidato)

    return hist_candidatos, tipo_pendiente_racha

def confirmar_cambio(ventana_confirmacion: int, metrica: str, historial_metrica: list[bool], verbose: bool = False) -> bool:
    """
    Confirma el Concept Drift si una cantidad suficiente de las últimas ventanas marcadas como candidatas pertenecen a la misma métrica.

    Args:
        ventana_confirmacion: Número de confirmaciones consecutivas necesarias.
        historiales: Diccionario de historiales de candidatos para cada métrica.

    Returns:
        True si se confirma un drift, False en caso contrario.
    """

    drift_confirmado = False

    if verbose:
        logger.debug(f"Ventana de confirmación: {ventana_confirmacion}")

    if len(historial_metrica) < ventana_confirmacion:
        if verbose:
            logger.debug(f"No hay suficientes datos para confirmar drift en {metrica} (tamaño historial: {len(historial_metrica)})")
        return False  # No hay suficientes datos para confirmar
    
    ultimos_candidatos = historial_metrica[-ventana_confirmacion:]

    if verbose:
        logger.debug(f"Últimos candidatos para {metrica}: {ultimos_candidatos}")

    if all(ultimos_candidatos):
        drift_confirmado = True
        logger.critical(f"   [!!!] DRIFT CONFIRMADO. Se procederá a recalcular el modelo.")
    
    return drift_confirmado


def obtener_traza_mas_nueva(ventana: pd.DataFrame) -> tuple:
    """
    Devuelve trace_real_index de la traza que entró
    más recientemente en la ventana (la de timestamp de inicio más tardío).
    Las trazas son siempre completas por filtrado previo.
    """
    primer_evento_por_traza = (
        ventana.sort_values('time:timestamp')
               .groupby('case:concept:name', sort=False)['time:timestamp']
               .min()
    )
    # La traza más nueva es la que empezó más tarde
    caso_mas_nuevo = primer_evento_por_traza.idxmax()
    fila_mas_nueva = ventana[ventana['case:concept:name'] == caso_mas_nuevo].iloc[0]
    return fila_mas_nueva['trace_real_index']

def deteccion_concept_drift(parametros: dict, nombre_metrica: str, valor_metrica: float, estado_metrica: dict, traza_mas_nueva: int) -> Tuple[bool, dict, Optional[int]]:
    """
    Detección de Concept Drift para una única métrica con estado independiente.

    Args:
        parametros: Diccionario de configuración para el drift.
        nombre_metrica: Nombre de la métrica actual.
        valor_metrica: Valor escalar de la métrica en la iteración actual.
        estado_metrica: Diccionario con el estado histórico de esta métrica específica.
        traza_mas_nueva: Índice de la traza más reciente.

    Returns:
        Tupla (drift_confirmado, estado_actualizado, traza_drift)
    """

    DEBUG = parametros['debug']

    # Inicializar listas históricas si no existen
    estado_metrica.setdefault('hist_valores', [])
    estado_metrica.setdefault('hist_candidatos', [])
    
    # Traza del primer candidato de la racha activa (None si no hay racha)
    tau_primer_candidato = estado_metrica.get('tau_primer_candidato', None)
    # Tipo de pendiente que inició la racha
    tipo_pendiente_racha = estado_metrica.get('tipo_pendiente_racha', None)

    # Registrar el valor actual
    estado_metrica['hist_valores'].append(valor_metrica)
    estado_metrica['hist_candidatos'], razon_cambio = identificar_candidato_cambio(
        parametros['n_regresion'], 
        estado_metrica['hist_valores'], 
        estado_metrica['hist_candidatos'], 
        tipo_pendiente_racha, 
        verbose=DEBUG
    )

    # ¿La iteración actual es candidata?
    es_candidato_ahora = estado_metrica['hist_candidatos'][-1] if estado_metrica['hist_candidatos'] else False
    logger.debug(f"[{nombre_metrica}] Marcada como candidata a drift: {es_candidato_ahora}")

    # Si no es candidata y no hay razón de cambio, resetear tipo de pendiente
    if not es_candidato_ahora and razon_cambio is None:
        tipo_pendiente_racha = None

    # Iteración anterior fue candidata
    hubo_candidato_previo = (
        len(estado_metrica['hist_candidatos']) > 1 and 
        estado_metrica['hist_candidatos'][-2]
    )
    logger.debug(f"[{nombre_metrica}] Iteración anterior fue candidata: {hubo_candidato_previo}")

    # Inicio de racha nueva: guardar τ_i solo una vez
    if es_candidato_ahora and not hubo_candidato_previo:
        tau_primer_candidato = traza_mas_nueva
        tipo_pendiente_racha = razon_cambio
        logger.debug(f"[{nombre_metrica}] [→] Racha iniciada en traza {tau_primer_candidato}")

    # Racha rota sin confirmación: limpiar
    if not es_candidato_ahora:
        tau_primer_candidato = None

    # Confirmar el drift basado en la ventana de confirmación
    drift_confirmado = confirmar_cambio(parametros['n_confirmacion'], nombre_metrica, estado_metrica['hist_candidatos'])
    
    traza_drift = None
    if drift_confirmado:
        # Obtener la traza más antigua de la última iteración (que causó la confirmación)
        traza_drift = tau_primer_candidato
        logger.warning(f"[{nombre_metrica}] Drift confirmado en traza: {traza_drift}")
        
    # Guardar estado para la siguiente iteración
    estado_metrica['tau_primer_candidato'] = tau_primer_candidato
    estado_metrica['tipo_pendiente_racha'] = tipo_pendiente_racha

    return drift_confirmado, estado_metrica, traza_drift
import pandas as pd
import pm4py
from typing import Optional, Tuple, Dict, Any
from ..logging_config import get_logger

logger = get_logger(__name__)

########################
# --- CONTROL-FLOW --- #
########################

# --- TAREAS DE FILTRADO ---
   
# Arriba


# --- TAREAS DE TRANSFORMACIÓN ---

def transformacion_simple(log: pd.DataFrame, parametros: dict) -> pd.DataFrame:
    """
    Aplica una transformación simple o nula al log de eventos. 
    Sirve para mantener el convenio del pipeline de procesamiento.

    Args:
        log: DataFrame con el log de eventos.
        parametros: Diccionario con parámetros de transformación.

    Returns:
        El DataFrame de eventos transformado o sin cambios.
    """
    return log

# --- TAREAS DE DESCUBRIMIENTO DE MODELO ---

def inductive_miner(log: pd.DataFrame, parametros: dict) -> dict:
    """
    Descubre una Red de Petri a partir del evento log usando el algoritmo Inductive Miner.

    Args:
        log: DataFrame con el evento log (formato pm4py).
        parametros: Diccionario con configuración (no se consultan parámetros propios).

    Returns:
        Diccionario con claves 'net', 'initial_marking' y 'final_marking' de la Red de Petri descubierta.
    """

    DEBUG = parametros.get('debug', False)

    logger.info("Descubriendo Red de Petri usando Inductive Miner")

    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log)


    # Guardar siempre la Red de Petri descubierta en la capa de modelos.
    nombre_archivo = f"./data/06_models/ControlFlow_PetirNet_InductiveMiner_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}"
    logger.info(f"Guardando el modelo descubierto como {nombre_archivo}.png")
    pm4py.save_vis_petri_net(net, initial_marking, final_marking, f"{nombre_archivo}.png")

    # Calcular el OLP para la precisión
    # Usar play_out básico para evitar caminos muy largos que no aportan al OLP y ralentizan la simulación.
    logger.info("Calculando OLP según simulator")
    log_simulado = pm4py.play_out(net, initial_marking, final_marking)
    dfg_modelo, _, _ = pm4py.discover_dfg(log_simulado)
    OLP = set(dfg_modelo.keys())

    if DEBUG:
         logger.info(f"OLP calculado: {OLP}")
         logger.info(f"Longitud OLP: {len(OLP)}")

    return {'net': net, 'initial_marking': initial_marking, 'final_marking': final_marking, 'OLP': OLP}

def heuristic_miner(log: pd.DataFrame, parametros: dict) -> dict:
    """
    Descubre una Red de Petri usando el algoritmo Heuristic Miner.
    
    Args:
        log: DataFrame con el evento log (formato pm4py).
        parametros: Diccionario con configuración.
    
    Returns:
        Un diccionario con las claves 'net', 'initial_marking' y 'final_marking' de la Red de Petri descubierta.
    """

    DEBUG = parametros.get('debug', False)

    # Descubrir la red de Petri usando el algoritmo heurístico.
    net, initial_marking, final_marking = pm4py.discover_petri_net_heuristics(log)

    # Guardar siempre la Red de Petri descubierta en la capa de modelos.
    nombre_archivo = f"./data/06_models/ControlFlow_PetirNet_HeuristicMiner_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}"
    logger.info(f"Guardando el modelo descubierto como {nombre_archivo}.png")
    pm4py.save_vis_petri_net(net, initial_marking, final_marking, f"{nombre_archivo}.png")

    # Calcular el OLP para la precisión
    logger.info("Calculando OLP según simulator")
    # Usar play_out básico para evitar caminos muy largos que no aportan al OLP y ralentizan la simulación.

    #log_simulado = simulator.apply(net, initial_marking, variant=simulator.Variants.BASIC_PLAYOUT, parameters={simulator.Variants.BASIC_PLAYOUT.value.Parameters.NO_TRACES: 1000})
    log_simulado = pm4py.play_out(net, initial_marking, final_marking)
    dfg_modelo, _, _ = pm4py.discover_dfg(log_simulado)
    OLP = set(dfg_modelo.keys())

    if DEBUG:
         logger.info(f"OLP calculado: {OLP}")
         logger.info(f"Longitud OLP: {len(OLP)}")

    return {'net': net, 'initial_marking': initial_marking, 'final_marking': final_marking, 'OLP': OLP}


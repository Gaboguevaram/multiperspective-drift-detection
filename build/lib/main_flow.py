import os
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"


import pandas as pd
import argparse
import time
import logging

from typing import Any, Dict, Optional
from prefect import flow, task
from pm4py.read import read_xes
from .settings import ARCHIVOS, NOISE_THRESHOLD, WINDOW_SIZE, WINDOW_JUMP
from .registro_op import REGISTRO_FILTRADO, REGISTRO_TRANSFORMACIONES, REGISTRO_MODELOS, REGISTRO_METRICAS, REGISTRO_DETECCION
from .ventana import extraccion_ventana, avanzar_ventana, comprobar_condicion_finalizacion
from .config import cargar_parametros
from .logging_config import setup_logging, get_logger
from pathlib import Path
from math import floor
from .concept_drift_detection import obtener_traza_mas_nueva

# Configurar el sistema de logging centralizado
setup_logging()
logger = get_logger(__name__)

# PLACEHOLDER
def evaluar_estado_global(resultados_iteracion: list):  
    """
    """
    total_resultados = len(resultados_iteracion)
    cambios_detectados = 0

    for resultado in resultados_iteracion:
        if resultado['cambio_detectado']:
            cambios_detectados+=1

    if cambios_detectados != 0:
            return True
    
    return False
  
#@task
def lanzar_iteracion(
    config: dict,
    iteracion: int,
    ventana: pd.DataFrame,
    nombre_perspectiva: str,
    cambio_detectado : bool = False, 
    modelo_actual: Any  = None,
    historial_metricas: dict = None,
    historial_cambios: list = None,
    traza_mas_nueva: int = None
    ) -> dict :
    """
    Ejecuta una única iteración del procesamiento para una perspectiva específica.

    Args:
        config: Configuración de la perspectiva actual.
        iteracion: Índice de la iteración actual.
        log_original: Log de eventos completo o segmentado de la iteración anterior.
        primero: Indicador booleano para la primera iteración.
        nombre_perspectiva: Nombre de la perspectiva que se está procesando.
        cambio_detectado: Si se detectó drift en la iteración anterior.
        modelo_actual: Modelo descubierto en la iteración anterior.
        estado_temporal: Estado de la ventana temporal/de eventos (inicio/final).

    Returns:
        Un diccionario con el estado actualizado después de la iteración.
    """
    
    logger.info(f"INICIANDO ITERACIÓN {iteracion + 1} para la perspectiva {nombre_perspectiva}")

    # --- TAREAS DE FILTRADO ---

    operaciones_filtrado = config.get('op_filtrado')
 
    for nombre_operacion in operaciones_filtrado:

        nombre_operacion = nombre_operacion.strip()

        if nombre_operacion in REGISTRO_FILTRADO:

            funcion_filtrado = REGISTRO_FILTRADO[nombre_operacion]

            ventana = funcion_filtrado(ventana, config)

        else:

            logger.error(f"La operación de filtrado '{nombre_operacion}' no existe.")

    """
    if ventana.empty:
        logger.warning(f"La ventana resultante después del filtrado para la perspectiva {nombre_perspectiva} está vacía. Se salta el resto de tareas para esta iteración.")
        return {'nombre' : nombre_perspectiva,
                'modelo': modelo_actual, 
                'cambio_detectado' : cambio_detectado,
                'hist_metricas': historial_metricas,
                'hist_cambios' : historial_cambios}
    """
    # --- TAREAS DE TRANSFORMACIÓN ---

    operaciones_transformacion = config.get('op_transformaciones')

    if len(operaciones_transformacion) == 0:
        logger.debug("No hay operaciones de transformación definidas. Se pasa a la fase de descubrimiento de modelo.")
    
    for nombre_operacion in operaciones_transformacion:

        nombre_operacion = nombre_operacion.strip()

        if nombre_operacion in REGISTRO_TRANSFORMACIONES:

            funcion_transformacion = REGISTRO_TRANSFORMACIONES[nombre_operacion]

            ventana = funcion_transformacion(ventana, config)

        else:

            logger.error(f"La operación de transformación '{nombre_operacion}' no existe.")

    # --- TAREA DE DESCUBRIMIENTO DE MODELO ---

    if modelo_actual is None or cambio_detectado is True:
        
        operacion_modelo = config.get('modelo')

        if operacion_modelo in REGISTRO_MODELOS:
            
            funcion_modelo = REGISTRO_MODELOS[operacion_modelo]

            # Descubrir o redescubrir el modelo.
            modelo_actual = funcion_modelo(ventana, config)
            
        else:

            logger.error(f"La operación de descubrimiento de modelo '{operacion_modelo}' no existe.")

    # --- TAREAS DE CÁLCULO DE MÉTRICAS ---

    operaciones_metricas = config.get('metricas')

    metricas = dict[str, float]()
    
    for nombre_operacion in operaciones_metricas:

        nombre_operacion = nombre_operacion.strip()

        if nombre_operacion in REGISTRO_METRICAS:

            funcion_metrica = REGISTRO_METRICAS[nombre_operacion]

            # Calcular la métrica usando el log segmentado y el modelo actual.
            metrica_actual = funcion_metrica(ventana, modelo_actual, config, nombre_operacion)
            
            metricas[nombre_operacion] = metrica_actual

        else:

            logger.error(f"La operación de métrica '{nombre_operacion}' no existe.")

    # --- TAREA DE EVALUACIÓN DE CONCEPT DRIFT ---

    operacion_deteccion = config.get('op_det_concept_drift')
    
    if operacion_deteccion in REGISTRO_DETECCION:
        
        funcion_deteccion = REGISTRO_DETECCION[operacion_deteccion]

        # Inicializar historial_metricas como contenedor de estados individuales si no existe
        if historial_metricas is None:
            historial_metricas = {}
        
        cambio_detectado_global = False
        metrica_con_cambio = None
        traza_drift_final = None

        # Llamar a deteccion_concept_drift una vez por cada métrica
        for nombre_metrica, valor_metrica in metricas.items():
            
            # Obtener o inicializar el estado individual de esta métrica
            if nombre_metrica not in historial_metricas:
                historial_metricas[nombre_metrica] = {}
            
            # Detectar drift para esta métrica específica
            drift_en_metrica, historial_metricas[nombre_metrica], traza_drift = funcion_deteccion(
                config, 
                nombre_metrica, 
                valor_metrica, 
                historial_metricas[nombre_metrica], 
                traza_mas_nueva
            )

            # Acumular: si alguna métrica detecta cambio, el global es True
            if drift_en_metrica:
                cambio_detectado_global = True
                metrica_con_cambio = nombre_metrica
                traza_drift_final = traza_drift

        # Registrar el cambio si se detectó
        if cambio_detectado_global:

            # Resetear el los hitoriales de todas las métricas para que la próxima iteración comience con un estado limpio.
            for nombre_metrica in metricas.keys():
                # Resetear contadores tras confirmar drift
                historial_metricas[nombre_metrica]['hist_candidatos'].clear()
                historial_metricas[nombre_metrica]['hist_valores'].clear()
                historial_metricas[nombre_metrica]['tau_primer_candidato'] = None
                historial_metricas[nombre_metrica]['tipo_pendiente_racha'] = None

            registro_cambio = {
                'cambio_detectado': cambio_detectado_global,
                'iteracion': iteracion + 1,
                'trace_real_index': traza_drift_final,
                'metrica': metrica_con_cambio  # Métrica que detectó el cambio
            }

            logger.warning(
                f"Concept drift detectado en la iteración {iteracion + 1} "
                f"para la perspectiva {nombre_perspectiva} - Métrica: {metrica_con_cambio}"
            )
            logger.info(f"Registro del cambio: {registro_cambio}")
            historial_cambios.append(registro_cambio)
        
        cambio_detectado = cambio_detectado_global

    else:
        logger.error(f"La operación de evaluación de concept drift '{operacion_deteccion}' no existe.")

    # Retornar todo el estado necesario para la siguiente iteración del orquestador.
    return {'nombre' : nombre_perspectiva,
            'modelo': modelo_actual, 
            'cambio_detectado' : cambio_detectado,
            'hist_metricas': historial_metricas,
            'hist_cambios' : historial_cambios}

#@flow
def orquestador_multidimensional(config: dict, log: Optional[pd.DataFrame] = None) -> dict:
    """
    Orquestador principal para el análisis de drift utilizando múltiples perspectivas concurrentemente.

    Args:
        config: Diccionario de configuración completo que contiene configuraciones globales y por perspectiva.
    """

    # Cargar la configuración global
    parametros_globales = config['configuracion_global']

    # Cargar el log
    if log is None:
        ruta_log = parametros_globales['ruta_log']
        extension_log = os.path.splitext(ruta_log)[1].lower()
        if extension_log == '.xes':
            log_original = read_xes(parametros_globales['ruta_log'])
        elif extension_log == '.csv':
            log_original = pd.read_csv(parametros_globales['ruta_log'])
        log_original['trace_real_index'] = pd.factorize(log_original['case:concept:name'])[0]
        logger.info(f"Log cargado: {parametros_globales['ruta_log']}")
    else:
        log_original = log
        logger.info("Log proporcionado como argumento.")

    # Cargar el número de iteraciones (si se define)
    iter_max = parametros_globales.get('max_iter', float('inf'))

    DEBUG = parametros_globales['debug']

    # Cargar las perspectivas solicitadas
    perspectivas = config['perspectivas']

    # Diccionario con los modelos actuales, inicializados a None
    modelos_actuales = {p['nombre']: None for p in perspectivas}

    # Diccionario con los estados temporales de cada modelo, inicializados a None
    estado_temporal = {'inicio' : None, 'fin': None}

    # Historial resultados
    hist_resultados_perspectivas = {p['nombre']: {} for p in perspectivas}

    # Historial cambios detectados
    hist_cambios_perspectivas = {p['nombre']: [] for p in perspectivas}

    # Diccionario con los resultados de cada iteración (si se detectó cambio o no)
    cambio_detecado = {p['nombre']: False for p in perspectivas}

    FIN_EJECUCION = False
    PRIMERO = True
    iteracion = 0

    while (not FIN_EJECUCION) and (iteracion < iter_max):
        
        logger.info(f"INICIANDO ITERACIÓN {iteracion + 1}")

        # Manejar avance de ventana si no es la primera iteración.
        if PRIMERO:
            ventana, estado_temporal = extraccion_ventana(log_original, parametros_globales, parametros_globales['ventana'])
            PRIMERO = False
        else:
            # Avanzar la ventana usando la función delegada y actualizar el estado temporal.
            ventana, estado_temporal = avanzar_ventana(log_original, ventana, parametros_globales, parametros_globales['ventana'], estado_temporal)
            
        FIN_EJECUCION = comprobar_condicion_finalizacion(log_original, ventana, DEBUG)

        if ventana.empty:
                logger.warning("La ventana está vacía. Salto a la siguiente iteración.")
                continue
        
        traza_mas_nueva = obtener_traza_mas_nueva(ventana)

        # Se delega la ejecución de cada perspectiva a una tarea submit.
        tareas_pendientes = []
        for p in perspectivas:
            # Fusionar la configuración de la perspectiva con la global.
            config_perspectiva = p | parametros_globales | estado_temporal
            # Submits la tarea lanzar_iteracion para ejecución concurrente.
            
            if config_perspectiva['avance'] == 'on_trace' and estado_temporal['traza_nueva'] == False:
                logger.info(f"No se ha detectado entrada de traza nueva en la iteración {iteracion + 1}. No se lanza iteración para la perspectiva {p['nombre']}.")
                continue
            
            #tareas = lanzar_iteracion.submit(config_perspectiva, iteracion, ventana, p['nombre'], CAMBIO_DETECTADO, modelos_actuales[p['nombre']], hist_resultados_perspectivas[p['nombre']], hist_cambios_perspectivas[p['nombre']], traza_mas_nueva)
            tareas = lanzar_iteracion(config_perspectiva, iteracion, ventana, p['nombre'], cambio_detecado[p['nombre']], modelos_actuales[p['nombre']], hist_resultados_perspectivas[p['nombre']], hist_cambios_perspectivas[p['nombre']], traza_mas_nueva)
            tareas_pendientes.append(tareas)

        # Esperar resultados de todas las tareas concurrentes.
        #resultados_iteracion = [tarea.result() for tarea in tareas_pendientes]
        resultados_iteracion = [tarea for tarea in tareas_pendientes]

        for resultado in resultados_iteracion:
            nombre = resultado['nombre']

            # Actualizar listado de modelos
            modelos_actuales[nombre] = resultado['modelo']

            # Actualizar el historial de resultados
            hist_resultados_perspectivas[nombre] = resultado['hist_metricas']

            hist_cambios_perspectivas[nombre] = resultado['hist_cambios']

            if len(resultado['hist_cambios']) > 0 and resultado['hist_cambios'][-1]['iteracion'] == iteracion + 1:
                cambio_detecado[nombre] = resultado['hist_cambios'][-1]['cambio_detectado']
            else:
                cambio_detecado[nombre] = False

            if cambio_detecado[nombre]:
                logger.warning(f"Se ha detectado un cambio en la perspectiva '{nombre}' en la iteración {iteracion + 1}. Redescubriendo modelo en la siguiente iteración.")

        iteracion += 1

    return hist_cambios_perspectivas
    
# Main
if __name__ == "__main__":

    # Configuración de los parámetros de entrada
    parser = argparse.ArgumentParser(description='Herramienta de minería de procesos multiperspectiva')

    # Fichero de entrada para los parametros
    parser.add_argument('-f', '--file', help='Indica el nombre del fichero donde se almacenan los parámetros de la ejecución',type=str, default=None)

    # Indicador de la ejecución a realizar
    parser.add_argument('-r', '--run_id', help='Indica o varias ejecuciones concretas a realizar',type=str, default=None)

    # Modo monoperspectiva
    parser.add_argument('-m', '--mono_perspectiva', action='store_true')

    args = parser.parse_args()

    # Cargar los parámetros del .yml
    parametros = cargar_parametros(args.file)

    if args.mono_perspectiva:
        
        # Ejecución monoperspectiva: utiliza solo la primera perspectiva definida.
        parametros_globales = parametros['configuracion_global']

        orquestador_multidimensional(parametros['perspectivas'][0] | parametros_globales)

    else:

        # Ejecución multidimensional/multiperspectiva.
        cambios_detectados_perspectivas = orquestador_multidimensional(parametros)

        for perspectiva in cambios_detectados_perspectivas:

            logger.info(f"Cambios detectados para la perspectiva '{perspectiva}':")

            cambios = cambios_detectados_perspectivas[perspectiva]
            logger.info(f"Cambios detectados por el algoritmo (sin mapeo):")
            for cambio in cambios:
                logger.info(f"{cambio}")

            logger.info(f"Cambios totales detectados: {len(cambios)}")

            indices_detectados_exactos = [registro_cambio['trace_real_index'] for registro_cambio in cambios]

            print(f"Indices detectados exactos: {indices_detectados_exactos}")

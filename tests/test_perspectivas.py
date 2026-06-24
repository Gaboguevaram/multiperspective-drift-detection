import re
import argparse
import pandas as pd
import logging
import os

import pm4py

os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

from math import ceil
from pm4py.read import read_xes
from src.main_flow import orquestador_multidimensional
from src.config import cargar_parametros
from src.logging_config import setup_logging, get_logger

# Configurar el sistema de logging centralizado
setup_logging()
logger = get_logger(__name__)

def _normalizar_recurso(nombre) -> str:
    # 'resource_Marta' / 'Marta' -> 'marta' (comparación robusta al prefijo y a mayúsculas)
    return str(nombre).replace('resource_', '').strip().lower()


def _norm_clave(clave):
    """Normaliza una clave de ground truth/detección: recurso (str) o par (recurso, tarea)."""
    if isinstance(clave, tuple):
        return tuple(_normalizar_recurso(x) for x in clave)
    return _normalizar_recurso(clave)


def calcular_metricas_tesis(cambios_reales: list, detecciones: list, tolerancia: int,
                            recurso_esperado=None, recursos_por_deteccion=None,
                            claves_esperadas=None):
    """
    Evalúa las detecciones con la ventana de tolerancia de la tesis.

    Dos modos:
    - Sin clave esperada (control_flow, arrival, service): el ground truth son las trazas de
      `cambios_reales`; un TP es una detección dentro de tolerancia de un cambio aún no cubierto.
    - Con clave esperada (perspectivas por-clave): la clave puede ser un recurso (str; calendar,
      utilization) o un par (recurso, tarea) (productividad). El ground truth es el producto
      {cambio_real} x {clave_esperada}; cada detección aporta un TP por cada clave esperada que
      cubra dentro de tolerancia, y un FP por cada clave NO esperada o duplicada. `recurso_esperado`
      (str) se mantiene por retrocompatibilidad y equivale a `claves_esperadas={recurso_esperado}`.
      `recursos_por_deteccion` es la lista paralela con el conjunto de claves de cada detección.
    """
    if claves_esperadas is None and recurso_esperado is not None:
        claves_esperadas = {recurso_esperado}

    if recursos_por_deteccion is None:
        recursos_por_deteccion = [None] * len(detecciones)

    TP = 0
    FP = 0
    retardos = []

    if claves_esperadas is None:
        # --- Ground truth por traza ---
        cambios_ya_detectados = set()
        for detectado in detecciones:
            es_tp = False
            for real in cambios_reales:
                if (real - tolerancia) <= detectado <= (real + tolerancia):
                    if real not in cambios_ya_detectados:
                        TP += 1
                        cambios_ya_detectados.add(real)
                        retardos.append(abs(detectado - real))
                        es_tp = True
                    break
            if not es_tp:
                FP += 1
        FN = len(cambios_reales) - len(cambios_ya_detectados)
    else:
        # --- Ground truth por (traza, clave esperada) ---
        esperadas_norm = {_norm_clave(c) for c in claves_esperadas}
        objetivos = {(real, c) for real in cambios_reales for c in esperadas_norm}
        cubiertos = set()
        for detectado, claves_det in zip(detecciones, recursos_por_deteccion):
            claves_norm = {_norm_clave(c) for c in (claves_det or [])}
            if not claves_norm:
                FP += 1  # detección sin clave en una perspectiva por-clave
                continue
            for clave in claves_norm:
                emparejado = None
                if clave in esperadas_norm:
                    for real in cambios_reales:
                        if (real - tolerancia) <= detectado <= (real + tolerancia) and (real, clave) not in cubiertos:
                            emparejado = real
                            break
                if emparejado is not None:
                    cubiertos.add((emparejado, clave))
                    TP += 1
                    retardos.append(abs(detectado - emparejado))
                else:
                    FP += 1  # clave no esperada, fuera de tolerancia, o duplicada
        FN = len(objetivos) - len(cubiertos)

    precision = (TP / (TP + FP)) if (TP + FP) > 0 else 0.0
    recall = (TP / (TP + FN)) if (TP + FN) > 0 else 0.0
    f_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    retardo_medio = sum(retardos) / len(retardos) if retardos else 0.0
    return TP, FP, FN, precision, recall, f_score, retardo_medio

def probar_control_flow(ruta_log: str, config: dict, tolerancia_error: float, nombre_perspectiva: str = 'control_flow'):

    cambios = 0
    indices_detectados_exactos = []
    tolerancia = 0
    indices_detectados_exactos = []

    # Cargar (csv o xes)
    extension_log = os.path.splitext(ruta_log)[1].lower()
    if extension_log == '.xes':
        # Algunos logs sintéticos (p. ej. cm-2500 de Maaradji) intercalan dos logs
        # base reutilizando los `case:concept:name`. pm4py preserva las apariciones
        # físicas en el EventLog, pero al convertir a DataFrame las fusiona por id
        # silenciosamente. Asignamos trace_real_index por orden físico de aparición
        # ANTES de convertir, para que cada traza física tenga un identificador único
        # en el DataFrame. El resto del pipeline identifica trazas por trace_real_index.
        event_log = pm4py.read_xes(ruta_log, return_legacy_log_object=True)
        for i, trace in enumerate(event_log):
            for event in trace:
                event['trace_real_index'] = i
        log_original = pm4py.convert_to_dataframe(event_log)
    elif extension_log == '.csv':
        log_original = pd.read_csv(ruta_log)
        # Mismo razonamiento que el XES: los CSV pueden tener case ids repetidos
        # entre dos apariciones físicas.
        # Se ordenan, se calcula el trace_real_index por orden de aparición física, y se reordenan por timestamp para el resto del pipeline.
        log_original.sort_values(by='case:concept:name', 
                        key=lambda x: x.str.extract(r'(\d+)', expand=False).astype(int), inplace=True)
        nueva_traza = log_original['case:concept:name'] != log_original['case:concept:name'].shift()
        log_original['trace_real_index'] = nueva_traza.cumsum() - 1
        log_original.sort_values(by='time:timestamp', inplace=True)
        
    # Obtener el nombre del log
    patron = r"/([^/]+)\."

    nombre_log = re.search(patron, ruta_log).group(1)

    # Obtener el número de trazas
    num_trazas = int(re.findall(r'\d+', ruta_log)[-1])

    # Tipo de cambio -> puntos de cambio reales.
    # 'single'/'sudden': un único cambio al 50%. 'recurring' y logs periódicos de
    # Maaradji (sin sufijo): un cambio cada 10%.
    if 'single' in ruta_log or 'sudden' in ruta_log:
        salto = int(num_trazas * 0.5)
    else:
        salto = int(num_trazas * 0.1)

    tolerancia = int(num_trazas * tolerancia_error)

    # Generamos la lista de cambios reales (Ej: [2500] para single; [250, 500, ...] para periódico)
    cambios_reales = list(range(salto, num_trazas, salto))
    
    logger.info(f"Analizando log: {ruta_log} (Tamaño: {num_trazas} trazas)")
    logger.info(f"Cambios reales esperados: {cambios_reales}")
    logger.info(f"Tolerancia admitida: ±{tolerancia} trazas")

    cambios_detectados_perspectivas = orquestador_multidimensional(config, log_original)
    cambios = cambios_detectados_perspectivas[nombre_perspectiva]

    logger.info(f"Cambios detectados por el algoritmo (sin mapeo):")
    for cambio in cambios:
        logger.info(f"{cambio}")
    
    logger.info(f"Cambios totales detectados: {len(cambios)}")

    # La detección se toma directamente del resultado del orquestador (vale para cualquier
    # tipo de ventana, incluida la temporal).
    indices_detectados_exactos = [registro_cambio['trace_real_index'] for registro_cambio in cambios]

    # Recursos que dispararon cada detección (aplanando recursos_con_cambio: {metrica: [recursos]}).
    # En perspectivas escalares (control_flow) queda vacío y no se comprueba el recurso.
    recursos_por_deteccion = []
    for registro_cambio in cambios:
        recursos = set()
        for lista_recursos in (registro_cambio.get('recursos_con_cambio') or {}).values():
            recursos.update(lista_recursos)
        recursos_por_deteccion.append(recursos)

    # Ground truth por clave del caso. Tres posibilidades:
    #   - recurso_esperado + tareas_esperadas -> claves = pares (recurso, tarea)   [productividad]
    #   - solo recurso_esperado               -> claves = {recurso}                [calendar, utilization]
    #   - ninguno                             -> None (solo traza)                 [control_flow, arrival, service]
    # Las claves de recursos_con_cambio ya son tuplas (recurso, tarea) en productividad
    # y strings (recurso) en calendar/utilization, así que casan con las claves esperadas.
    cg = config.get('configuracion_global') or {}
    recurso_esperado = cg.get('recurso_esperado')
    tareas_esperadas = cg.get('tareas_esperadas')
    if recurso_esperado and tareas_esperadas:
        claves_esperadas = {(recurso_esperado, tarea) for tarea in tareas_esperadas}
    elif recurso_esperado:
        claves_esperadas = {recurso_esperado}
    else:
        claves_esperadas = None

    print(f"Indices detectados exactos: {indices_detectados_exactos}")
    if claves_esperadas is not None:
        print(f"Claves esperadas: {claves_esperadas} | claves por detección: {recursos_por_deteccion}")

    # Calcular resultados
    TP, FP, FN, precision, recall, f_score, retardo = calcular_metricas_tesis(
        cambios_reales,
        indices_detectados_exactos,
        tolerancia,
        claves_esperadas=claves_esperadas,
        recursos_por_deteccion=recursos_por_deteccion,
    )
    # Mostrar los resultados por pantalla y guardarlos en un archivo
    mostrar_metricas_tesis(TP, FP, FN, precision, recall, f_score, retardo, nombre_log, config, cambios_reales, indices_detectados_exactos, tolerancia, cambios)

    print(f_score)

    return f_score

def mostrar_metricas_tesis(TP, FP, FN, precision, recall, f_score, retardo, nombre_log=None, config=None, cambios_reales=None, indices_detectados_exactos=None, tolerancia=None, cambios=None):

    # Imprimir el informe final
    logger.info("\n" + "="*40)
    logger.info("      RESULTADOS DEL TEST")
    logger.info("="*40)
    logger.info(f" Verdaderos Positivos (TP) : {TP}")
    logger.info(f" Falsos Positivos     (FP) : {FP}")
    logger.info(f" Falsos Negativos     (FN) : {FN}")
    logger.info("-" * 40)
    logger.info(f" Precision : {precision:.4f}")
    logger.info(f" Recall    : {recall:.4f}")
    logger.info(f" F-Score   : {f_score:.4f}")
    logger.info(f" Retardo (Δ): {retardo:.2f} trazas")
    logger.info("="*40)


    # Escribirlo en un archivo
    nombre_archivo = f"./resultados/resultados_test_{nombre_log}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(nombre_archivo, "w", encoding="utf-8") as f:

        f.write(f"{config}\n")

        f.write(f"Cambios reales esperados: {cambios_reales}\n")
        f.write(f"Tolerancia admitida: ±{tolerancia} trazas\n")

        f.write(f"Cambios detectados por el algoritmo: {indices_detectados_exactos}\n")
        f.write(f"Longitud del resultado: {len(cambios)}\n")

        f.write("\n" + "="*40 + "\n")
        f.write("      RESULTADOS DEL BENCHMARK\n")
        f.write("="*40 + "\n")
        f.write(f" Verdaderos Positivos (TP) : {TP}\n")
        f.write(f" Falsos Positivos     (FP) : {FP}\n")
        f.write(f" Falsos Negativos     (FN) : {FN}\n")
        f.write("-" * 40 + "\n")
        f.write(f" Precision : {precision:.4f}\n")
        f.write(f" Recall    : {recall:.4f}\n")
        f.write(f" F-Score   : {f_score:.4f}\n")
        f.write(f" Retardo (Δ): {retardo:.2f} trazas\n")
        f.write("="*40 + "\n")

        logger.info(f"Resultados guardados en {nombre_archivo}")

def probar_arrival_rate(ruta_log: str, config: dict, tolerancia_error: float, nombre_perspectiva: str = 'arrival_rate'):

    cambios = []
    indices_detectados_exactos = []
    tolerancia = 0
    indices_detectados_exactos = []

    # Cargar
    extension_log = os.path.splitext(ruta_log)[1].lower()
    if extension_log == '.xes':
        log = read_xes(ruta_log)
    elif extension_log == '.csv':
        log = pd.read_csv(ruta_log)
    log['trace_real_index'] = (log['case:concept:name'] != log['case:concept:name'].shift(1)).cumsum()
    logger.info(f"Log cargado: {ruta_log}")

    # Obtener el nombre del log
    patron = r"/([^/]+)\."

    nombre_log = re.search(patron, ruta_log).group(1)

    # Obtener el número de trazas
    num_trazas = int(re.findall(r'\d+', ruta_log)[-1])

    # Saber si el log contiene cambios o no
    patron_nc = r"nc"

    resultado = re.search(patron_nc, ruta_log)
    if not resultado:
        # Generar la lista de cambios reales (Ej: [250, 500, 750...])
        salto = int(num_trazas * 0.1)
        tolerancia = int(num_trazas * tolerancia_error)
        cambios_reales = list(range(salto, num_trazas, salto))
    else:
        cambios_reales = []

    logger.info(f"Analizando log: {ruta_log} (Tamaño: {num_trazas} trazas)")
    logger.info(f"Cambios reales esperados: {cambios_reales}")
    logger.info(f"Tolerancia admitida: ±{tolerancia} trazas")

    cambios_detectados_perspectivas = orquestador_multidimensional(config, log)
    cambios = cambios_detectados_perspectivas['arrival_rate']

    logger.info(f"Cambios detectados por el algoritmo (sin mapeo):")
    for cambio in cambios:
        logger.info(f"{cambio}")
    
    indices_detectados_exactos = [registro_cambio['trace_real_index'] for registro_cambio in cambios]

    logger.info(f"Indices detectados exactos: {indices_detectados_exactos}")

    if not cambios_reales:

        if len(cambios) == 0:
            logger.info("No se han detectado cambios, como era de esperar.")
            mostrar_metricas_tesis(0, 0, 0, 1.0, 1.0, 1.0, 0.0, nombre_log, config, cambios_reales, indices_detectados_exactos, tolerancia, cambios)
        else:
            logger.info(f"Se han detectado {len(cambios)} cambios, lo cual es un falso positivo.")
            mostrar_metricas_tesis(0, len(cambios), 0, 0.0, 1.0, 0.0, 0.0, nombre_log, config, cambios_reales, indices_detectados_exactos, tolerancia, cambios)

    else:

        # Calcular resultados
        TP, FP, FN, precision, recall, f_score, retardo = calcular_metricas_tesis(
            cambios_reales, 
            indices_detectados_exactos, 
            tolerancia
        )
        # Mostrar los resultados por pantalla y guardarlos en un archivo
        mostrar_metricas_tesis(TP, FP, FN, precision, recall, f_score, retardo, nombre_log, config, cambios_reales, indices_detectados_exactos, tolerancia, cambios)

        print(f_score)

        return f_score
    

def probar_service_rate(ruta_log: str, config: dict, tolerancia_error: float):

    cambios = 0
    indices_detectados_exactos = []
    tolerancia = 0
    indices_detectados_exactos = []

    # Cargar (csv o xes)
    extension_log = os.path.splitext(ruta_log)[1].lower()
    if extension_log == '.xes':
        log = read_xes(ruta_log)
    elif extension_log == '.csv':
        log = pd.read_csv(ruta_log)

    # Índice físico de traza (ordenando por id numérico y luego por tiempo),
    # robusto frente a ids reutilizados en los logs sintéticos.
    log.sort_values(by='case:concept:name',
                    key=lambda x: x.str.extract(r'(\d+)', expand=False).astype(int), inplace=True)
    nueva_traza = log['case:concept:name'] != log['case:concept:name'].shift()
    log['trace_real_index'] = nueva_traza.cumsum() - 1
    log.sort_values(by='time:timestamp', inplace=True)

    # Obtener el nombre del log
    patron = r"/([^/]+)\."

    nombre_log = re.search(patron, ruta_log).group(1)

    # Obtener el número de trazas
    num_trazas = int(re.findall(r'\d+', ruta_log)[-1])

    # Tipo de cambio -> puntos de cambio reales.
    # 'single'/'sudden': un único cambio al 50%. 'recurring' y logs periódicos de
    # Maaradji (sin sufijo): un cambio cada 10%.
    if 'single' in ruta_log or 'sudden' in ruta_log:
        salto = int(num_trazas * 0.5)
    else:
        salto = int(num_trazas * 0.1)

    tolerancia = int(num_trazas * tolerancia_error)

    # Generamos la lista de cambios reales (Ej: [2500] para single; [250, 500, ...] para periódico)
    cambios_reales = list(range(salto, num_trazas, salto))
    
    logger.info(f"Analizando log: {ruta_log} (Tamaño: {num_trazas} trazas)")
    logger.info(f"Cambios reales esperados: {cambios_reales}")
    logger.info(f"Tolerancia admitida: ±{tolerancia} trazas")

    cambios_detectados_perspectivas = orquestador_multidimensional(config, log)
    cambios = cambios_detectados_perspectivas['control_flow']

    logger.info(f"Cambios detectados por el algoritmo (sin mapeo):")
    for cambio in cambios:
        logger.info(f"{cambio}")
    
    logger.info(f"Cambios totales detectados: {len(cambios)}")

    # La detección se toma directamente del resultado del orquestador (vale para cualquier
    # tipo de ventana, incluida la temporal).
    indices_detectados_exactos = [registro_cambio['trace_real_index'] for registro_cambio in cambios]

    # Recursos que dispararon cada detección (aplanando recursos_con_cambio: {metrica: [recursos]}).
    # En perspectivas escalares (control_flow) queda vacío y no se comprueba el recurso.
    recursos_por_deteccion = []
    for registro_cambio in cambios:
        recursos = set()
        for lista_recursos in (registro_cambio.get('recursos_con_cambio') or {}).values():
            recursos.update(lista_recursos)
        recursos_por_deteccion.append(recursos)

    # Ground truth por clave del caso. Tres posibilidades:
    #   - recurso_esperado + tareas_esperadas -> claves = pares (recurso, tarea)   [productividad]
    #   - solo recurso_esperado               -> claves = {recurso}                [calendar, utilization]
    #   - ninguno                             -> None (solo traza)                 [control_flow, arrival, service]
    # Las claves de recursos_con_cambio ya son tuplas (recurso, tarea) en productividad
    # y strings (recurso) en calendar/utilization, así que casan con las claves esperadas.
    cg = config.get('configuracion_global') or {}
    recurso_esperado = cg.get('recurso_esperado')
    tareas_esperadas = cg.get('tareas_esperadas')
    if recurso_esperado and tareas_esperadas:
        claves_esperadas = {(recurso_esperado, tarea) for tarea in tareas_esperadas}
    elif recurso_esperado:
        claves_esperadas = {recurso_esperado}
    else:
        claves_esperadas = None

    print(f"Indices detectados exactos: {indices_detectados_exactos}")
    if claves_esperadas is not None:
        print(f"Claves esperadas: {claves_esperadas} | claves por detección: {recursos_por_deteccion}")

    # Calcular resultados
    TP, FP, FN, precision, recall, f_score, retardo = calcular_metricas_tesis(
        cambios_reales,
        indices_detectados_exactos,
        tolerancia,
        claves_esperadas=claves_esperadas,
        recursos_por_deteccion=recursos_por_deteccion,
    )
    # Mostrar los resultados por pantalla y guardarlos en un archivo
    mostrar_metricas_tesis(TP, FP, FN, precision, recall, f_score, retardo, nombre_log, config, cambios_reales, indices_detectados_exactos, tolerancia, cambios)

    print(f_score)

    return f_score


def probar_calendar(ruta_log: str, config: dict, tolerancia_error: float):

    cambios = []
    indices_detectados_exactos = []
    tolerancia = 0
    indices_detectados_exactos = []

    # Cargar
    extension_log = os.path.splitext(ruta_log)[1].lower()
    if extension_log == '.xes':
        log = read_xes(ruta_log)
    elif extension_log == '.csv':
        log = pd.read_csv(ruta_log)

    # Agregamos el índice real de la traza al log para poder mapear los cambios detectados con los cambios reales
    log.sort_values(by='case:concept:name', 
                            key=lambda x: x.str.extract(r'(\d+)', expand=False).astype(int), inplace=True)
    nueva_traza = log['case:concept:name'] != log['case:concept:name'].shift()
    log['trace_real_index'] = nueva_traza.cumsum() - 1
    log.sort_values(by='time:timestamp', inplace=True)

    logger.info(f"Log cargado: {ruta_log}")
    log.to_csv('./tests/log_debug_test.csv', index=False)

    # Obtener el nombre del log
    patron = r"/([^/]+)\."

    nombre_log = re.search(patron, ruta_log).group(1)

    # Obtener el número de trazas
    num_trazas = int(re.findall(r'\d+', ruta_log)[-1])

    #Obtener el tipo de cambio (sudden o recurring)
    if 'sudden' in ruta_log:
        salto = int(num_trazas * 0.5)
    elif 'recurring' in ruta_log:
        salto = int(num_trazas * 0.1)
    else:
        raise ValueError("No se pudo determinar el tipo de cambio a partir del nombre del log. Asegúrate de que el nombre contenga 'sudden' o 'recurring'.")

    tolerancia = int(num_trazas * tolerancia_error)
    # Generar la lista de cambios reales (Ej: [250, 500, 750...])
    cambios_reales = list(range(salto, num_trazas, salto))

    logger.info(f"Analizando log: {ruta_log} (Tamaño: {num_trazas} trazas)")
    logger.info(f"Cambios reales esperados: {cambios_reales}")
    logger.info(f"Tolerancia admitida: ±{tolerancia} trazas")

    cambios_detectados_perspectivas = orquestador_multidimensional(config, log)
    cambios = cambios_detectados_perspectivas['calendar']

    logger.info(f"Cambios detectados por el algoritmo (sin mapeo):")
    for cambio in cambios:
        logger.info(f"{cambio}")
    
    indices_detectados_exactos = [registro_cambio['trace_real_index'] for registro_cambio in cambios]

    logger.info(f"Indices detectados exactos: {indices_detectados_exactos}")

    # Calcular resultados
    TP, FP, FN, precision, recall, f_score, retardo = calcular_metricas_tesis(
        cambios_reales, 
        indices_detectados_exactos, 
        tolerancia
    )
    
    # Mostrar los resultados por pantalla y guardarlos en un archivo
    mostrar_metricas_tesis(TP, FP, FN, precision, recall, f_score, retardo, nombre_log, config, cambios_reales, indices_detectados_exactos, tolerancia, cambios)

    print(f_score)

    return f_score

def probar_resource_utilization(ruta_log: str, config: dict, tolerancia_error: float):

    cambios = []
    indices_detectados_exactos = []
    tolerancia = 0
    indices_detectados_exactos = []

    # Cargar
    extension_log = os.path.splitext(ruta_log)[1].lower()
    if extension_log == '.xes':
        log = read_xes(ruta_log)
    elif extension_log == '.csv':
        log = pd.read_csv(ruta_log)

    # Agregamos el índice real de la traza al log para poder mapear los cambios detectados con los cambios reales
    log.sort_values(by='case:concept:name', 
                            key=lambda x: x.str.extract(r'(\d+)', expand=False).astype(int), inplace=True)
    nueva_traza = log['case:concept:name'] != log['case:concept:name'].shift()
    log['trace_real_index'] = nueva_traza.cumsum() - 1
    log.sort_values(by='time:timestamp', inplace=True)

    logger.info(f"Log cargado: {ruta_log}")
    log.to_csv('./tests/log_debug_test.csv', index=False)

    # Obtener el nombre del log
    patron = r"/([^/]+)\."

    nombre_log = re.search(patron, ruta_log).group(1)

    # Obtener el número de trazas
    num_trazas = int(re.findall(r'\d+', ruta_log)[-1])

    #Obtener el tipo de cambio (sudden o recurring)
    if 'sudden' in ruta_log:
        salto = int(num_trazas * 0.5)
    elif 'recurring' in ruta_log:
        salto = int(num_trazas * 0.1)
    else:
        raise ValueError("No se pudo determinar el tipo de cambio a partir del nombre del log. Asegúrate de que el nombre contenga 'sudden' o 'recurring'.")

    tolerancia = int(num_trazas * tolerancia_error)
    # Generar la lista de cambios reales (Ej: [250, 500, 750...])
    cambios_reales = list(range(salto, num_trazas, salto))

    logger.info(f"Analizando log: {ruta_log} (Tamaño: {num_trazas} trazas)")
    logger.info(f"Cambios reales esperados: {cambios_reales}")
    logger.info(f"Tolerancia admitida: ±{tolerancia} trazas")

    cambios_detectados_perspectivas = orquestador_multidimensional(config, log)
    cambios = cambios_detectados_perspectivas['resource_utilization']

    logger.info(f"Cambios detectados por el algoritmo (sin mapeo):")
    for cambio in cambios:
        logger.info(f"{cambio}")
    
    indices_detectados_exactos = [registro_cambio['trace_real_index'] for registro_cambio in cambios]

    logger.info(f"Indices detectados exactos: {indices_detectados_exactos}")

    # Calcular resultados
    TP, FP, FN, precision, recall, f_score, retardo = calcular_metricas_tesis(
        cambios_reales, 
        indices_detectados_exactos, 
        tolerancia
    )
    
    # Mostrar los resultados por pantalla y guardarlos en un archivo
    mostrar_metricas_tesis(TP, FP, FN, precision, recall, f_score, retardo, nombre_log, config, cambios_reales, indices_detectados_exactos, tolerancia, cambios)

    print(f_score)

    return f_score

def probar_resource_productivity(ruta_log: str, config: dict, tolerancia_error: float):

    cambios = []
    indices_detectados_exactos = []
    tolerancia = 0
    indices_detectados_exactos = []

    # Cargar
    extension_log = os.path.splitext(ruta_log)[1].lower()
    if extension_log == '.xes':
        log = read_xes(ruta_log)
    elif extension_log == '.csv':
        log = pd.read_csv(ruta_log)

    # Agregamos el índice real de la traza al log para poder mapear los cambios detectados con los cambios reales
    log.sort_values(by='case:concept:name', 
                            key=lambda x: x.str.extract(r'(\d+)', expand=False).astype(int), inplace=True)
    nueva_traza = log['case:concept:name'] != log['case:concept:name'].shift()
    log['trace_real_index'] = nueva_traza.cumsum() - 1
    log.sort_values(by='time:timestamp', inplace=True)

    logger.info(f"Log cargado: {ruta_log}")
    log.to_csv('./tests/log_debug_test.csv', index=False)

    # Obtener el nombre del log
    patron = r"/([^/]+)\."

    nombre_log = re.search(patron, ruta_log).group(1)

    # Obtener el número de trazas
    num_trazas = int(re.findall(r'\d+', ruta_log)[-1])

    #Obtener el tipo de cambio (sudden o recurring)
    if 'sudden' in ruta_log:
        salto = int(num_trazas * 0.5)
    elif 'recurring' in ruta_log:
        salto = int(num_trazas * 0.1)
    else:
        raise ValueError("No se pudo determinar el tipo de cambio a partir del nombre del log. Asegúrate de que el nombre contenga 'sudden' o 'recurring'.")

    tolerancia = int(num_trazas * tolerancia_error)
    # Generar la lista de cambios reales (Ej: [250, 500, 750...])
    cambios_reales = list(range(salto, num_trazas, salto))

    logger.info(f"Analizando log: {ruta_log} (Tamaño: {num_trazas} trazas)")
    logger.info(f"Cambios reales esperados: {cambios_reales}")
    logger.info(f"Tolerancia admitida: ±{tolerancia} trazas")

    cambios_detectados_perspectivas = orquestador_multidimensional(config, log)
    cambios = cambios_detectados_perspectivas['resource_productivity']

    logger.info(f"Cambios detectados por el algoritmo (sin mapeo):")
    for cambio in cambios:
        logger.info(f"{cambio}")
    
    indices_detectados_exactos = [registro_cambio['trace_real_index'] for registro_cambio in cambios]

    logger.info(f"Indices detectados exactos: {indices_detectados_exactos}")

    # Calcular resultados
    TP, FP, FN, precision, recall, f_score, retardo = calcular_metricas_tesis(
        cambios_reales, 
        indices_detectados_exactos, 
        tolerancia
    )
    
    # Mostrar los resultados por pantalla y guardarlos en un archivo
    mostrar_metricas_tesis(TP, FP, FN, precision, recall, f_score, retardo, nombre_log, config, cambios_reales, indices_detectados_exactos, tolerancia, cambios)

    print(f_score)

    return f_score
    

if __name__ == "__main__":

    # Configuración de los parámetros de entrada
    parser = argparse.ArgumentParser(description='Herramienta de minería de procesos multiperspectiva')

    # Fichero log
    parser.add_argument('-l', '--log', help='Indica el nombre del log sobre el cual hacer el test ',type=str, default="./data/01_raw/ORI-2500.xes")

    # Perspectiva a probar
    parser.add_argument('-p', '--perspectiva', help='Indica la perspectiva a probar (control_flow, service_rate, arrival_rate)', type=str, default="control_flow")

    # Archivo de configuración
    parser.add_argument('-f', '--file', help='Indica el archivo de configuración .yml a usar', type=str, default="./conf/ventana_trazas.yml")

    args = parser.parse_args()

    tolerancia_error = 0.05

    if args.perspectiva == 'control_flow':
        probar_control_flow(args.log,cargar_parametros(args.file), tolerancia_error, nombre_perspectiva='control_flow')
    elif args.perspectiva == 'arrival_rate':
        probar_control_flow(args.log,cargar_parametros(args.file), tolerancia_error, nombre_perspectiva='arrival_rate')
    elif args.perspectiva == 'service_rate':
        probar_control_flow(args.log,cargar_parametros(args.file), tolerancia_error, nombre_perspectiva='service_rate')
    elif args.perspectiva == 'calendar':
        probar_control_flow(args.log,cargar_parametros(args.file), tolerancia_error, nombre_perspectiva='calendar')
    elif args.perspectiva == 'resource_utilization':
        probar_control_flow(args.log,cargar_parametros(args.file), tolerancia_error, nombre_perspectiva='resource_utilization')
    elif args.perspectiva == 'resource_productivity':
        probar_control_flow(args.log,cargar_parametros(args.file), tolerancia_error, nombre_perspectiva='resource_productivity')




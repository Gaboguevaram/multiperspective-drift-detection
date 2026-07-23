import re
import argparse
import pandas as pd
import os

import pm4py

# Modo efímero de Prefect por defecto. Ver la nota extensa de src/main_flow.py:
# se vacía PREFECT_API_URL (en vez de dejarla sin definir) para que el perfil del
# usuario no imponga un servidor que quizá no esté levantado. Si el usuario exporta
# PREFECT_API_URL explícitamente, se respeta.
if not os.environ.get("PREFECT_API_URL"):
    os.environ["PREFECT_API_URL"] = ""
os.environ.setdefault("PREFECT_SERVER_ALLOW_EPHEMERAL_MODE", "true")
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

from src.main_flow import orquestador_multidimensional
from src.config import cargar_parametros, asegurar_directorios_salida
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

def probar_perspectiva(ruta_log: str, config: dict, tolerancia_error: float, nombre_perspectiva: str = 'control_flow'):

    # Crear las capas de data/ y los directorios de salida si no existen.
    asegurar_directorios_salida()

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


if __name__ == "__main__":

    # Configuración de los parámetros de entrada
    parser = argparse.ArgumentParser(description='Herramienta de minería de procesos multiperspectiva')

    # Fichero log
    parser.add_argument('-l', '--log', help='Indica el nombre del log sobre el cual hacer el test ', type=str, default="./data/01_raw/control_flow/single/cb-5000-single.csv")

    # Perspectiva a probar
    parser.add_argument('-p', '--perspectiva', help='Indica la perspectiva a probar (control_flow, arrival_rate, service_rate, calendar, resource_utilization, resource_productivity)', type=str, default="control_flow")

    # Archivo de configuración
    parser.add_argument('-f', '--file', help='Indica el archivo de configuración .yml a usar', type=str, default="./conf/logs_simples/cf_cb.yml")

    args = parser.parse_args()

    tolerancia_error = 0.05

    probar_perspectiva(args.log, cargar_parametros(args.file), tolerancia_error, nombre_perspectiva=args.perspectiva)




from collections import deque
import os

from prefect import task, flow
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

import pandas as pd
import pm4py
import argparse
import logging

from typing import Any, Dict, Optional, Union
from .registro import REGISTRO_FILTRADO, REGISTRO_TRANSFORMACIONES, REGISTRO_MODELOS, REGISTRO_METRICAS, REGISTRO_DETECCION
from .registro import DEPENDENCIAS_ENTRE_PERSPECTIVAS
from .ventana import extraccion_ventana, avanzar_ventana, comprobar_condicion_finalizacion, validar_ventanas, resolver_ventana_perspectiva
from .config import cargar_parametros
from .logging_config import setup_logging, get_logger
from .concept_drift_detection import obtener_traza_mas_nueva
from .ajuste_ventana import (
    ajustar_tamano_ventana,
    posicion_inicial_desde_traza,
    derivar_n_confirmacion_regresion,
    calcular_salto_equivalente_traza,
)

# Configurar el sistema de logging centralizado
setup_logging()
logger = get_logger(__name__)


@task(name="Lanzar iteración", retries=1, retry_delay_seconds=5)
def lanzar_iteracion(
    config: dict,
    iteracion: int,
    ventana: pd.DataFrame,
    nombre_perspectiva: str,
    cambio_detectado : bool = False, 
    modelo_actual: Any  = None,
    historiales_metricas: dict = None,
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

    if modelo_actual is None or cambio_detectado:

        operacion_modelo = config.get('modelo')

        # Si el drift de la iteración anterior fue por-recurso (métricas dict),
        # recuperamos del último registro la lista de recursos afectados para que
        # los modelos que lo soporten (p. ej. modelo_calendarios) hagan un
        # redescubrimiento selectivo en lugar de reconstruir todo desde cero.
        recursos_a_redescubrir = None
        if cambio_detectado and historial_cambios:
            ultimo_cambio = historial_cambios[-1]
            recursos_por_metrica = ultimo_cambio.get('recursos_con_cambio') or {}
            if recursos_por_metrica:
                recursos_a_redescubrir = sorted({
                    r for lista in recursos_por_metrica.values() for r in lista
                })

        if operacion_modelo in REGISTRO_MODELOS:

            funcion_modelo = REGISTRO_MODELOS[operacion_modelo]

            if recursos_a_redescubrir and modelo_actual is not None:
                # Pasamos info para reentrenamiento selectivo a través del config.
                # Los modelos que no las consulten las ignoran sin efecto.
                config_modelo = config | {
                    '_recursos_a_redescubrir': recursos_a_redescubrir,
                    '_modelo_anterior': modelo_actual,
                }
                modelo_actual = funcion_modelo(ventana, config_modelo)
            else:
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

    # Guardar los valores de las métricas de esta iteración a la capa de salidas de modelo.
    if config.get('debug', False) and metricas:
        ruta_metricas = f"./data/07_model_output/{nombre_perspectiva}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_metricas.csv"
        pd.DataFrame([{'iteracion': iteracion + 1, **metricas}]).to_csv(ruta_metricas, index=False)

    # --- TAREA DE EVALUACIÓN DE CONCEPT DRIFT ---

    operacion_deteccion = config.get('op_det_concept_drift')

    if operacion_deteccion in REGISTRO_DETECCION:

        funcion_deteccion = REGISTRO_DETECCION[operacion_deteccion]

        # Inicializar historiales_metricas como contenedor de estados individuales si no existe
        if historiales_metricas is None:
            historiales_metricas = {}

        # --- RESET POR CAMBIO DE ESTADO POR DEPENDENCIA ---
        if config.get('estado_cambiado_por_dependencia'):
            logger.info(
                f"[{nombre_perspectiva}] reseteando historial de métricas: el estado "
                f"fue actualizado por una dependencia."
            )
            historiales_metricas.clear()

        cambio_detectado_global = False
        metrica_con_cambio = None
        traza_drift_final = None
        # Para métricas dict (p. ej. soporte por recurso), guardamos qué claves
        # confirmaron drift en esta iteración. Se persiste en el registro_cambio
        # para que la siguiente iteración pueda hacer reentrenamiento selectivo.
        recursos_con_cambio_por_metrica: dict[str, list] = {}

        for nombre_metrica, valor_metrica in metricas.items():

            if isinstance(valor_metrica, dict):
                # Métrica por-clave (p. ej. por-recurso): estado anidado clave -> estado_individual.
                # Si el estado previo era escalar (tiene 'hist_valores'), lo reinicializamos como
                # contenedor de sub-estados para no mezclar semánticas.
                estado_actual = historiales_metricas.get(nombre_metrica)
                if not isinstance(estado_actual, dict) or 'hist_valores' in estado_actual:
                    historiales_metricas[nombre_metrica] = {}
                estado_por_clave = historiales_metricas[nombre_metrica]

                claves_con_drift = []

                for clave, valor_clave in valor_metrica.items():
                    # Recurso nuevo nunca visto: empezamos a llevar historial a partir de ahora.
                    if clave not in estado_por_clave:
                        estado_por_clave[clave] = {}

                    drift_en_clave, estado_por_clave[clave], traza_drift = funcion_deteccion(
                        config,
                        f"{nombre_metrica}::{clave}",
                        valor_clave,
                        estado_por_clave[clave],
                        traza_mas_nueva,
                    )

                    if drift_en_clave:
                        claves_con_drift.append(clave)
                        cambio_detectado_global = True
                        metrica_con_cambio = nombre_metrica
                        traza_drift_final = traza_drift

                # Las claves del estado que NO aparecen en valor_metrica este tick
                # se dejan intactas (racha congelada; se reanudará cuando reaparezcan).
                if claves_con_drift:
                    recursos_con_cambio_por_metrica[nombre_metrica] = claves_con_drift

            else:
                # Métrica escalar
                estado_actual = historiales_metricas.get(nombre_metrica)
                # Si previamente era nested (dict de sub-estados), reinicializamos como plano.
                if not isinstance(estado_actual, dict) or (estado_actual and 'hist_valores' not in estado_actual):
                    historiales_metricas[nombre_metrica] = {}

                drift_en_metrica, historiales_metricas[nombre_metrica], traza_drift = funcion_deteccion(
                    config,
                    nombre_metrica,
                    valor_metrica,
                    historiales_metricas[nombre_metrica],
                    traza_mas_nueva,
                )

                if drift_en_metrica:
                    cambio_detectado_global = True
                    metrica_con_cambio = nombre_metrica
                    traza_drift_final = traza_drift

        if cambio_detectado_global:
            # Reset selectivo según tipo de métrica:
            # - dict: solo se limpian las claves que confirmaron drift (las demás conservan
            #   su historial porque su parte del modelo no cambia).
            # - escalar: se limpia la métrica completa (se preserva la semántica original).
            for nombre_metrica, valor_metrica in metricas.items():
                if isinstance(valor_metrica, dict):
                    for clave in recursos_con_cambio_por_metrica.get(nombre_metrica, []):
                        estado_clave = historiales_metricas[nombre_metrica].get(clave)
                        if estado_clave is None:
                            continue
                        if 'hist_candidatos' in estado_clave:
                            estado_clave['hist_candidatos'].clear()
                        if 'hist_valores' in estado_clave:
                            estado_clave['hist_valores'].clear()
                        if 'tau_primer_candidato' in estado_clave:
                            estado_clave['tau_primer_candidato'] = None
                        if 'tipo_pendiente_racha' in estado_clave:
                            estado_clave['tipo_pendiente_racha'] = None
                else:
                    # Resetear contadores tras confirmar drift
                    if 'hist_candidatos' in historiales_metricas[nombre_metrica]:
                        historiales_metricas[nombre_metrica]['hist_candidatos'].clear()
                    if 'hist_valores' in historiales_metricas[nombre_metrica]:
                        historiales_metricas[nombre_metrica]['hist_valores'].clear()
                    if 'tau_primer_candidato' in historiales_metricas[nombre_metrica]:
                        historiales_metricas[nombre_metrica]['tau_primer_candidato'] = None
                    if 'tipo_pendiente_racha' in historiales_metricas[nombre_metrica]:
                        historiales_metricas[nombre_metrica]['tipo_pendiente_racha'] = None

            registro_cambio = {
                'cambio_detectado': cambio_detectado_global,
                'iteracion': iteracion + 1,
                'trace_real_index': traza_drift_final,
                'metrica': metrica_con_cambio,
                'recursos_con_cambio': recursos_con_cambio_por_metrica,
            }

            if recursos_con_cambio_por_metrica:
                logger.warning(
                    f"Concept drift detectado en la iteración {iteracion + 1} "
                    f"para la perspectiva {nombre_perspectiva} - Métrica: {metrica_con_cambio} "
                    f"- Recursos: {recursos_con_cambio_por_metrica}"
                )
            else:
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
            'hist_metricas': historiales_metricas,
            'hist_cambios' : historial_cambios}

def _aplicar_ajuste_ventana(
    log_original: pd.DataFrame,
    nombre_perspectiva: str,
    config_perspectiva: dict,
    parametros_globales: dict,
    parametros_ventana: dict,
    momento: str,
    hist_cambios: Optional[list] = None,
) -> bool:
    """
    Calibra `tamano_ventana` para una perspectiva con `autoajuste: true`, y recalibra
    los parámetros del detector `n_regresion` y `n_confirmacion` en consecuencia.

    `salto_ventana` se calcula UNA SOLA VEZ durante el ajuste inicial (a partir del
    log, como el desplazamiento equivalente a "una traza" en el tipo de ventana
    correspondiente; ver `calcular_salto_equivalente_traza`) y queda fijo durante toda
    la ejecución. En el ajuste post-drift, `salto_ventana` no se toca.

    El parámetro `momento` selecciona desde qué punto del log arranca el algoritmo
    de búsqueda del tamaño óptimo y si se calcula también `salto_ventana`:

    - 'inicial':    el ajuste se ejecuta ANTES del primer tick del orquestador.
                    No existe historial de drift; el algoritmo arranca desde el
                    comienzo del análisis (`fecha_inicial`/`primer_evento`/
                    `primera_traza` declarado en el YAML, o el inicio natural del
                    log si no se declaran). Además, `salto_ventana` se sobrescribe
                    con el valor equivalente a una traza.

    - 'post-drift': el ajuste se ejecuta DURANTE la ejecución, justo después de que
                    el detector confirme un drift. El algoritmo arranca desde la
                    traza donde se confirmó ese drift (leída del último entry de
                    `hist_cambios`). Requiere pasar `hist_cambios`. `salto_ventana`
                    se conserva intacto.

    En ambos momentos, tras actualizar `tamano_ventana` (y opcionalmente `salto_ventana`),
    se recalibran `n_regresion`/`n_confirmacion` vía `derivar_n_confirmacion_regresion`.
    Todas las mutaciones son in-place sobre los dicts pasados por argumento; el
    orquestador, al fusionarlos en cada iteración, recoge los nuevos valores
    automáticamente.

    Devuelve True si se modificó `tamano_ventana`, False en caso contrario
    (autoajuste desactivado, posición de partida no determinable, o el algoritmo
    no devolvió un tamaño distinto al actual).
    """
    # Filtro previo común: si la perspectiva no tiene autoajuste activo, salir.
    if not parametros_ventana.get('autoajuste', False):
        return False

    # Etiqueta de log usada en todas las trazas de esta llamada. El sufijo identifica
    # el momento del ciclo de vida en que se disparó el ajuste para facilitar el
    # diagnóstico cuando ambos modos coexisten en una misma ejecución.
    contexto_log = (
        'ajuste_ventana_inicial' if momento == 'inicial' else 'ajuste_ventana'
    )

    # --- Determinar la posición de partida del algoritmo según el momento. ---
    posicion_inicial: Optional[Union[int, pd.Timestamp]] = None
    # Texto descriptivo del disparo, usado en el log final para trazabilidad.
    detalle_disparo = ''

    if momento == 'inicial':
        # Sin historial previo: usar `fecha_inicial`/`primer_evento`/`primera_traza`
        # del YAML, o el comienzo natural del log si no se declararon.
        tipo_ventana = parametros_ventana.get('tipo')

        if tipo_ventana == 'temporal':
            fecha_inicial_yaml = parametros_ventana.get('fecha_inicial')
            if fecha_inicial_yaml is not None:
                posicion_inicial = pd.Timestamp(fecha_inicial_yaml)
            else:
                ts_col = pd.to_datetime(
                    log_original['time:timestamp'], errors='coerce'
                ).dropna()
                if not ts_col.empty:
                    posicion_inicial = pd.Timestamp(ts_col.min())

        elif tipo_ventana == 'eventos':
            primer_evento_yaml = parametros_ventana.get('primer_evento')
            posicion_inicial = int(primer_evento_yaml) if primer_evento_yaml is not None else 0

        elif tipo_ventana == 'trazas':
            primera_traza_yaml = parametros_ventana.get('primera_traza')
            posicion_inicial = int(primera_traza_yaml) if primera_traza_yaml is not None else 0

        if posicion_inicial is None:
            logger.warning(
                f"[{contexto_log}] '{nombre_perspectiva}': no se pudo determinar "
                f"la posición de partida (tipo='{tipo_ventana}'). Se omite el ajuste."
            )
            return False

        detalle_disparo = (
            f'calibración previa al primer tick desde posicion_inicial={posicion_inicial}'
        )

    elif momento == 'post-drift':
        # Requiere historial de cambios: la posición es la traza donde se confirmó
        # el último drift.
        if not hist_cambios:
            return False

        ultimo_cambio = hist_cambios[-1]
        trace_idx = ultimo_cambio.get('trace_real_index')
        if trace_idx is None:
            logger.warning(
                f"[{contexto_log}] '{nombre_perspectiva}' confirmó drift pero el registro "
                f"no contiene trace_real_index; se omite el ajuste."
            )
            return False

        posicion_inicial = posicion_inicial_desde_traza(
            log_original, trace_idx, parametros_ventana['tipo']
        )
        if posicion_inicial is None:
            return False

        detalle_disparo = f'drift en traza {trace_idx}'

    else:
        raise ValueError(
            f"_aplicar_ajuste_ventana: momento desconocido '{momento}'. "
            f"Valores admitidos: 'inicial', 'post-drift'."
        )

    # --- Ejecutar el algoritmo de ajuste con la posición de partida calculada. ---
    config_combinada = config_perspectiva | parametros_globales
    nuevo_tamano = ajustar_tamano_ventana(
        log_original, posicion_inicial, config_combinada, parametros_ventana,
    )

    # Si no se pudo calcular un nuevo tamaño se escoge el tamaño actual
    if nuevo_tamano is None:
        nuevo_tamano = parametros_ventana.get('tamano_ventana')

    # Aplicar el nuevo tamano_ventana y salto_ventana
    tamano_anterior = parametros_ventana.get('tamano_ventana')
    salto_anterior = parametros_ventana.get('salto_ventana')
    parametros_ventana['tamano_ventana'] = nuevo_tamano

    # salto_ventana se calcula UNA SOLA VEZ, durante el ajuste inicial
    if momento == 'inicial':
        nuevo_salto = calcular_salto_equivalente_traza(
            log_original, parametros_ventana['tipo']
        )
        if nuevo_salto is not None:
            parametros_ventana['salto_ventana'] = nuevo_salto
        else:
            logger.warning(
                f"[{contexto_log}] '{nombre_perspectiva}': no se pudo calcular el "
                f"salto equivalente a una traza para tipo='{parametros_ventana['tipo']}'. "
                f"Se conserva el salto_ventana declarado en el YAML ({salto_anterior})."
            )

    salto_actual = parametros_ventana['salto_ventana']
    if momento == 'inicial':
        salto_log = f"salto_ventana {salto_anterior} -> {salto_actual}"
    else:
        salto_log = f"salto_ventana={salto_actual} (fijo desde el ajuste inicial)"

    logger.warning(
        f"[{contexto_log}] '{nombre_perspectiva}': tamano_ventana "
        f"{tamano_anterior} -> {nuevo_tamano}, {salto_log} ({detalle_disparo})."
    )

    # Recalibrar n_confirmacion y n_regresion en config_perspectiva.
    derivados = derivar_n_confirmacion_regresion(parametros_ventana)
    if derivados is not None:
        n_conf_nuevo, n_reg_nuevo = derivados
        n_conf_anterior = config_perspectiva.get('n_confirmacion')
        n_reg_anterior = config_perspectiva.get('n_regresion')
        config_perspectiva['n_confirmacion'] = n_conf_nuevo
        config_perspectiva['n_regresion'] = n_reg_nuevo
        logger.warning(
            f"[{contexto_log}] '{nombre_perspectiva}': "
            f"n_confirmacion {n_conf_anterior} -> {n_conf_nuevo}, "
            f"n_regresion {n_reg_anterior} -> {n_reg_nuevo}."
        )
    else:
        logger.info(
            f"[{contexto_log}] '{nombre_perspectiva}': no se pudo recalibrar "
            f"n_confirmacion/n_regresion (cociente tamano/salto inválido)."
        )

    return True

def _procesar_resultados(
    resultados_iteracion: list,
    iteracion: int,
    modelos_actuales: dict,
    hist_resultados_perspectivas: dict,
    hist_cambios_perspectivas: dict,
    cambio_detecado: dict,
    dependencias: dict,
    estado_temporal: dict,
) -> None:
    """
    Vuelca los resultados producidos en una iteración por las perspectivas que dispararon
    a los diccionarios de estado del orquestador.

    Las perspectivas que NO dispararon en este tick (modo multi-ventana) no aparecen
    en `resultados_iteracion`, por lo que su entrada en `cambio_detecado` se mantiene
    intacta. Esto preserva un drift detectado en un tick previo hasta que la perspectiva
    vuelva a ejecutarse y procese el redescubrimiento del modelo.
    """
    for resultado in resultados_iteracion:

        nombre = resultado['nombre']


        # Comprobar si es el primero modelo en ser descubierto
        modelo_anterior = modelos_actuales[nombre]
        es_primer_modelo = modelo_anterior is None and resultado['modelo'] is not None

        # Actualizar el modelo actual de la perspectiva (puede ser un redescubrimiento si hubo drift).
        modelos_actuales[nombre] = resultado['modelo']

        # Actualizar el historial de métricas/estados de detección por perspectiva.
        hist_resultados_perspectivas[nombre] = resultado['hist_metricas']

        # Actualizar el historial de cambios detectados por perspectiva.
        hist_cambios_perspectivas[nombre] = resultado['hist_cambios']

        # El flag `cambio_detectado` solo es True si el último cambio registrado pertenece
        # a esta misma iteración; en otro caso, se resetea para no redescubrir dos veces.
        if len(resultado['hist_cambios']) > 0 and resultado['hist_cambios'][-1]['iteracion'] == iteracion + 1:
            cambio_detecado[nombre] = resultado['hist_cambios'][-1]['cambio_detectado']
        else:
            cambio_detecado[nombre] = False

        # Solo consumidoras declaradas en esta ejecución.
        perspectivas_dependientes = [
            clave for clave, valor in DEPENDENCIAS_ENTRE_PERSPECTIVAS.items()
            if nombre in valor and clave in dependencias
        ]
        productor = nombre

        for consumidor in perspectivas_dependientes:

            # Encolar si hay pendiente acumulado de un drift previo, O si es el primer
            # modelo del productor en esta ejecución. 
            if dependencias[consumidor][productor]['pendiente'] or es_primer_modelo:

                multi_ventana = 'inicio' not in estado_temporal

                estado_productor = estado_temporal[productor] if multi_ventana else estado_temporal

                _procesar_dependencias(
                    productor=productor,
                    consumidor=consumidor,
                    dependencias=dependencias[consumidor][productor],
                    modelo_productor=modelos_actuales[productor],
                    estado_productor=estado_productor,
                    es_primer_modelo=es_primer_modelo)

                dependencias[consumidor][productor]['pendiente'] = False
                           

        if cambio_detecado[nombre]:

            logger.warning(
                f"Se ha detectado un cambio en la perspectiva '{nombre}' en la iteración "
                f"{iteracion + 1}. Redescubriendo modelo en la siguiente iteración."
            )

            if perspectivas_dependientes:
                logger.warning(
                    f"Se deben modificar las perspectivas dependientes de {productor}: {perspectivas_dependientes}." 
                    f"Se hará en la siguiente iteración"
                )

                for consumidor in perspectivas_dependientes:

                    dependencias[consumidor][productor]['pendiente'] = True

def _procesar_dependencias(
        productor: str,
        consumidor: str,
        dependencias: dict,
        modelo_productor: Any,
        estado_productor: dict,
        es_primer_modelo: bool = False
    ):
    """
    Procesa la dependencia entre un productor y un consumidor, actualizando la lista de
    modelos pendientes del consumidor según el estado del productor.

    Args:
        productor: Nombre de la perspectiva productora.
        consumidor: Nombre de la perspectiva consumidora.
        dependencias: Diccionario con las dependencias entre perspectivas.
        modelo_productor: Modelo actualizado del productor.
        estado_productor: Estado del productor.
        es_primer_modelo: Indica si es el primer modelo descubierto.

    Returns:
        None
    """

    if productor == 'calendar':

        logger.info(f"Procesando dependencia entre productor '{productor}' y consumidor '{consumidor}'.")

        # Obtener el modelo actualizado del productor (calendarios)
        calendarios_pendientes = dependencias.get('calendarios_pendientes', None)

        if calendarios_pendientes is None:
            dependencias['calendarios_pendientes'] = deque()

        if es_primer_modelo:

            logger.info(f"El modelo del productor '{productor}' es el primer modelo descubierto."
                        f"Se considera válido desde el inicio del análisis.")
            
            # None = "válido desde siempre".
            validez = None
            dependencias['calendarios_pendientes'].append({
                'calendario': modelo_productor,
                'validez': validez,
            })

        else: 

            logger.info(
                f"El modelo del productor '{productor}' se ha actualizado. "
                f"Se añade a la lista de calendarios pendientes del consumidor '{consumidor}'"
                f"con validez desde {estado_productor['inicio']} hasta {estado_productor['fin']}.")

            dependencias['calendarios_pendientes'].append({
                'calendario': modelo_productor,
                'validez': estado_productor['fin'],
            })

def _orquestador_uni_ventana(
    log_original: pd.DataFrame,
    parametros_globales: dict,
    perspectivas: list,
    parametros_ventana: dict,
) -> dict:
    """
    Orquestador para el caso clásico (uni-ventana): una única configuración de ventana
    compartida por todas las perspectivas. En cada iteración se avanza esa ventana y se
    ejecutan TODAS las perspectivas activas sobre la misma porción del log.

    Soporta cualquier tipo de ventana (temporal, eventos, trazas), ya que aquí el avance
    es síncrono y no requiere comparar `fin` entre perspectivas.
    """
    # Cargar el número máximo de iteraciones (si se define en config; si no, sin tope).
    iter_max = parametros_globales.get('max_iter', float('inf'))

    DEBUG = parametros_globales['debug']

    # Diccionario con los modelos actuales de cada perspectiva, inicializados a None.
    modelos_actuales = {p['nombre']: None for p in perspectivas}

    # Historial de resultados (estados internos del detector) por perspectiva.
    hist_resultados_perspectivas = {p['nombre']: {} for p in perspectivas}

    # Historial de cambios detectados por perspectiva.
    hist_cambios_perspectivas = {p['nombre']: [] for p in perspectivas}

    # Flag por perspectiva indicando si en la última iteración se confirmó un drift.
    cambio_detecado = {p['nombre']: False for p in perspectivas}

    # Estado temporal compartido (inicio/fin de la ventana actual).
    estado_temporal = {'inicio': None, 'fin': None}

    # Diccionario con las dependencias entre perspectivas, usado para pasar info de una a otra en caso de drift.
    nombres_activos = {p['nombre'] for p in perspectivas}
    dependencias = {
        consumidor: {
            'estado_cambiado_por_dependencia': False,
            **{
                productor: {'pendiente': False}
                for productor in productores
                if productor in nombres_activos
            },
        }
        for consumidor, productores in DEPENDENCIAS_ENTRE_PERSPECTIVAS.items()
        if consumidor in nombres_activos
    }

    # Autoajuste INICIAL de la ventana (antes del primer tick).
    if len(perspectivas) > 1 and parametros_ventana.get('autoajuste', False):

        logger.warning(f"Se están empleando múltiples perspectivas con ventana compartida."
                       f"El modo autoajuste está deshabilitado para evitar conflictos. No se usará"
        )

        parametros_ventana['autoajuste'] = False

    for p in perspectivas:
        _aplicar_ajuste_ventana(
            log_original, p['nombre'],
            p, parametros_globales, parametros_ventana,
            momento='inicial',
        )

    FIN_EJECUCION = False
    PRIMERO = True
    iteracion = 0
    ventana = None

    while (not FIN_EJECUCION) and (iteracion < iter_max):

        logger.info(f"INICIANDO ITERACIÓN {iteracion + 1}")

        # Primera iteración: extraer la ventana inicial. Iteraciones posteriores: avanzar.
        if PRIMERO:
            ventana, estado_temporal = extraccion_ventana(log_original, parametros_globales, parametros_ventana)
            PRIMERO = False
        else:
            # Avanzar la ventana usando la función delegada y actualizar el estado temporal.
            ventana, estado_temporal = avanzar_ventana(log_original, ventana, parametros_globales, parametros_ventana, estado_temporal)

        # Comprobar si esta ventana ya alcanza el final del log; el bucle terminará tras esta iteración.
        FIN_EJECUCION = comprobar_condicion_finalizacion(log_original, ventana, DEBUG)

        if ventana.empty:
            logger.warning("La ventana está vacía. Salto a la siguiente iteración.")
            iteracion += 1
            continue

        # Identificador de la traza más reciente en la ventana (se usa para mapear drifts a trazas reales).
        traza_mas_nueva = obtener_traza_mas_nueva(ventana)

        # Se delega la ejecución de cada perspectiva a una tarea submit (desactivado: ejecución secuencial).
        tareas_pendientes = []
        for p in perspectivas:

            # Restricción de orden de ejecución: las perspectivas consumidoras no se lanzan
            # hasta que TODOS sus productores DECLARADOS hayan producido al menos un modelo.
            # Los productores no declarados se filtran (la consumidora opera autónomamente sobre ellos)
            # comprobando su presencia en `modelos_actuales`, que solo contiene perspectivas activas.
            listado_productores = [
                productor for productor in DEPENDENCIAS_ENTRE_PERSPECTIVAS.get(p['nombre'], [])
                if productor in modelos_actuales
            ]
            if any(modelos_actuales[productor] is None for productor in listado_productores):
                logger.info(
                    f"La perspectiva '{p['nombre']}' espera a que sus productores "
                    f"{listado_productores} produzcan modelo. No se lanza en la iteración {iteracion + 1}."
                )
                continue

            # Fusionar la configuración de la perspectiva con la global y el estado temporal.
            parametros_dependencias = dependencias.get(p['nombre'], {})
            config_perspectiva = p | parametros_globales | estado_temporal | parametros_dependencias

            # Si la perspectiva está configurada para "avance on_trace" y no entró traza nueva,
            # se omite esta iteración para esa perspectiva.
            if config_perspectiva.get('avance') == 'on_trace' and estado_temporal.get('traza_nueva') is False:
                logger.info(
                    f"No se ha detectado entrada de traza nueva en la iteración {iteracion + 1}. "
                    f"No se lanza iteración para la perspectiva {p['nombre']}."
                )
                continue

            # Submits la tarea lanzar_iteracion para ejecución concurrente con Prefect.
            # TODO: rehabilitar el modo .submit() cuando volvamos a usar Prefect; permite
            # ejecutar perspectivas en paralelo y recoger los futures con .result() abajo.
            #tarea = lanzar_iteracion.submit(config_perspectiva, iteracion, ventana, p['nombre'], cambio_detecado[p['nombre']], modelos_actuales[p['nombre']], hist_resultados_perspectivas[p['nombre']], hist_cambios_perspectivas[p['nombre']], traza_mas_nueva)
            
            tarea = lanzar_iteracion(
                config_perspectiva, iteracion, ventana, p['nombre'],
                cambio_detecado[p['nombre']], modelos_actuales[p['nombre']],
                hist_resultados_perspectivas[p['nombre']], hist_cambios_perspectivas[p['nombre']],
                traza_mas_nueva,
            )
            
            tareas_pendientes.append(tarea)

        # Esperar resultados de todas las tareas concurrentes.
        # En modo Prefect: tarea.result() bloquea hasta que el future tenga valor.
        #resultados_iteracion = [tarea.result() for tarea in tareas_pendientes]
        resultados_iteracion = [tarea for tarea in tareas_pendientes]

        # Volcar los resultados al estado del orquestador.
        _procesar_resultados(
            resultados_iteracion, iteracion,
            modelos_actuales, hist_resultados_perspectivas,
            hist_cambios_perspectivas, cambio_detecado, 
            dependencias, estado_temporal
        )

        # Autoajuste del tamaño de ventana tras un drift confirmado.
        for p in perspectivas:
            nombre = p['nombre']
            if cambio_detecado.get(nombre, False):
                # Solo ejecutar autoajuste si es la PRIMERA iteración tras el drift.
                # Verificar que el último cambio ocurrió en esta iteración actual.
                hist_cambios = hist_cambios_perspectivas[nombre]
                if len(hist_cambios) > 0 and hist_cambios[-1]['iteracion'] == iteracion + 1:
                    _aplicar_ajuste_ventana(
                        log_original, nombre,
                        p, parametros_globales, parametros_ventana,
                        momento='post-drift',
                        hist_cambios=hist_cambios,
                    )

                    # Burn-in post-drift
                    n_conf_burn = p.get('n_confirmacion')
                    if n_conf_burn:
                        # Claves (recurso/par) que confirmaron el drift en las métricas dict,
                        # para marcar el burn-in solo donde el modelo se remina (refit selectivo).
                        recursos_con_cambio = hist_cambios[-1].get('recursos_con_cambio', {})
                        for nombre_metrica, estado_metrica in hist_resultados_perspectivas[nombre].items():
                            if not isinstance(estado_metrica, dict):
                                continue
                            if 'hist_valores' in estado_metrica:
                                # Métrica escalar: estado plano, se marca directamente.
                                estado_metrica['burn_in'] = int(n_conf_burn)
                            else:
                                # Métrica por-clave (dict anidado clave -> sub-estado): solo las
                                # claves que confirmaron el drift sufren el transitorio del warm-up
                                # (el resto del modelo no se remina), así que solo ellas reciben flag.
                                for clave in recursos_con_cambio.get(nombre_metrica, []):
                                    sub_estado = estado_metrica.get(clave)
                                    if isinstance(sub_estado, dict) and 'hist_valores' in sub_estado:
                                        sub_estado['burn_in'] = int(n_conf_burn)

        iteracion += 1

    return hist_cambios_perspectivas


def _orquestador_multi_ventana(
    log_original: pd.DataFrame,
    parametros_globales: dict,
    perspectivas: list,
) -> dict:
    """
    Orquestador para el caso multi-ventana: cada perspectiva tiene su propia ventana temporal.

    Planificador: "fin mínimo". En cada tick avanzan únicamente las perspectivas cuya ventana
    actual termina antes (`fin` mínimo entre las activas). Cuando varias perspectivas coinciden
    en el mismo `fin`, todas avanzan a la vez. Esto sincroniza naturalmente perspectivas con
    horizontes muy distintos (ej. control_flow=1d y calendar=90d): la grande "espera" mientras
    la pequeña recorre su intervalo, y vuelven a coincidir cuando el `fin` de la pequeña alcanza
    al de la grande.

    Restricciones (validadas en `validar_ventanas` antes de entrar aquí):
    - Todas las ventanas deben ser de tipo 'temporal' (los `fin` se comparan como Timestamps).
    - Solo se admiten unidades de duración constante (días, horas, minutos, segundos, semanas).
    """
    # Cargar el número máximo de iteraciones (si se define en config; si no, sin tope).
    iter_max = parametros_globales.get('max_iter', float('inf'))

    DEBUG = parametros_globales['debug']

    # Lista de nombres en el orden declarado y mapeo nombre -> bloque de configuración.
    nombres = [p['nombre'] for p in perspectivas]
    config_por_nombre = {p['nombre']: p for p in perspectivas}

    # Resolver la configuración de ventana de cada perspectiva (con fallback a la global).
    parametros_ventana_por_nombre = {
        n: resolver_ventana_perspectiva(config_por_nombre[n], parametros_globales) for n in nombres
    }

    # Diccionario con los modelos actuales de cada perspectiva, inicializados a None.
    modelos_actuales = {n: None for n in nombres}

    # Historial de resultados (estados internos del detector) por perspectiva.
    hist_resultados_perspectivas = {n: {} for n in nombres}

    # Historial de cambios detectados por perspectiva.
    hist_cambios_perspectivas = {n: [] for n in nombres}

    # Flag por perspectiva indicando si en su última ejecución se confirmó un drift.
    # En multi-ventana NO se resetea entre ticks para perspectivas que no disparen,
    # de forma que el flag persiste hasta que la perspectiva vuelva a procesar y
    # redescubrir su modelo (ver _procesar_resultados).
    cambio_detecado = {n: False for n in nombres}

    # Marcador por perspectiva: True cuando su ventana alcanzó el final del log.
    perspectivas_terminadas = {n: False for n in nombres}

    # Estado por perspectiva: ventana actual (DataFrame) y estado temporal (inicio/fin).
    ventanas: Dict[str, pd.DataFrame] = {}
    estados_temporales: Dict[str, dict] = {}

    # Diccionario con las dependencias entre perspectivas.
    dependencias = {
        consumidor: {
            'estado_cambiado_por_dependencia': False,
            **{
                productor: {'pendiente': False}
                for productor in productores
                if productor in nombres
            },
        }
        for consumidor, productores in DEPENDENCIAS_ENTRE_PERSPECTIVAS.items()
        if consumidor in nombres
    }


    # Autoajuste INICIAL de la ventana (antes de la primera extracción).
    # Cada perspectiva con `autoajuste: true` calibra su tamaño de ventana
    # usando el inicio del log (o el `fecha_inicial`/`primer_evento`/`primera_traza`
    # del YAML) como semilla.
    for n in nombres:
        _aplicar_ajuste_ventana(
            log_original, n,
            config_por_nombre[n], parametros_globales,
            parametros_ventana_por_nombre[n],
            momento='inicial'
        )

    # --- AJUSTE DE COHERENCIA ENTRE VENTANAS DE PRODUCTORA Y CONSUMIDORA ---
    # AGRANDAMOS la ventana de la productora hasta igualar la
    # de la consumidora más grande que dependa de ella.
    for consumidor in nombres:
        listado_productores = DEPENDENCIAS_ENTRE_PERSPECTIVAS.get(consumidor, [])
        if not listado_productores:
            continue

        tamano_consumidor = parametros_ventana_por_nombre[consumidor]['tamano_ventana']

        for productor in listado_productores:
            if productor not in parametros_ventana_por_nombre:
                # Productor no declarado en esta ejecución: la consumidora opera de forma
                # autónoma (descubre su propio modelo). No hay nada que ajustar.
                continue

            params_productor = parametros_ventana_por_nombre[productor]
            tamano_productor = params_productor['tamano_ventana']

            if pd.Timedelta(tamano_productor) < pd.Timedelta(tamano_consumidor):
                logger.warning(
                    f"[ajuste_dependencias] '{consumidor}' tiene tamano_ventana={tamano_consumidor} "
                    f"pero su productora '{productor}' tiene tamano_ventana={tamano_productor}. "
                    f"Una ventana del consumidor abarcaría varios modelos del productor, lo que "
                    f"rompe la semántica de la dependencia. Se agranda tamano_ventana de "
                    f"'{productor}' a {tamano_consumidor} para igualar al de la consumidora."
                )
                params_productor['tamano_ventana'] = tamano_consumidor

                # Recalcular salto_ventana con la misma fórmula que el ajuste inicial
                # (desplazamiento equivalente a "una traza" en este tipo de ventana).
                nuevo_salto = calcular_salto_equivalente_traza(
                    log_original, params_productor['tipo']
                )
                if nuevo_salto is not None:
                    params_productor['salto_ventana'] = nuevo_salto

                # Recalibrar n_confirmacion y n_regresion del detector de la productora
                # con el nuevo cociente tamano_ventana / salto_ventana.
                derivados = derivar_n_confirmacion_regresion(params_productor)
                if derivados is not None:
                    n_conf_nuevo, n_reg_nuevo = derivados
                    config_por_nombre[productor]['n_confirmacion'] = n_conf_nuevo
                    config_por_nombre[productor]['n_regresion'] = n_reg_nuevo
                    logger.warning(
                        f"[ajuste_dependencias] '{productor}': "
                        f"n_confirmacion -> {n_conf_nuevo}, n_regresion -> {n_reg_nuevo}."
                    )

    # Extracción inicial: cada perspectiva arranca con su propia ventana.
    for n in nombres:
        v_inicial, e_inicial = extraccion_ventana(
            log_original, parametros_globales, parametros_ventana_por_nombre[n]
        )
        ventanas[n] = v_inicial
        estados_temporales[n] = e_inicial
        logger.info(
            f"Ventana inicial '{n}': inicio={e_inicial['inicio']} fin={e_inicial['fin']} "
            f"({len(v_inicial)} eventos)"
        )

    iteracion = 0
    primer_tick = True

    while iteracion < iter_max:
        # Activas = perspectivas que aún no han llegado al final del log.
        activas = [n for n in nombres if not perspectivas_terminadas[n]]
        if not activas:
            logger.info("Todas las perspectivas alcanzaron el final del log. Fin de la ejecución.")
            break

        if primer_tick:
            # En el primer tick TODAS las perspectivas disparan con su ventana inicial.
            disparan = list(activas)
            primer_tick = False
        else:
            # Regla "fin mínimo": avanza la(s) perspectiva(s) cuya ventana cierra antes.
            # Las ventanas con horizonte mayor (ej. calendar=90d) "esperan" en su sitio
            # mientras las de horizonte menor (ej. control_flow=1d) avanzan tick a tick.
            min_fin = min(estados_temporales[n]['fin'] for n in activas)
            disparan = [n for n in activas if estados_temporales[n]['fin'] == min_fin]

            # Avanzar la ventana SOLO de las perspectivas que disparan en este tick.
            for n in disparan:
                ventana_anterior = ventanas[n]
                v_nueva, e_nueva = avanzar_ventana(
                    log_original, ventana_anterior, parametros_globales,
                    parametros_ventana_por_nombre[n], estados_temporales[n],
                )
                ventanas[n] = v_nueva
                estados_temporales[n] = e_nueva

        logger.info(f"INICIANDO ITERACIÓN {iteracion + 1}. Perspectivas que disparan: {disparan}")

        # Comprobar fin de log por perspectiva (la marca como terminada para no volver a avanzar
        # en ticks futuros, pero aún así se ejecuta sobre esta última ventana).
        for n in disparan:
            if comprobar_condicion_finalizacion(log_original, ventanas[n], DEBUG):
                logger.info(f"Perspectiva '{n}' alcanzó el final del log en la iteración {iteracion + 1}.")
                perspectivas_terminadas[n] = True

        # Se delega la ejecución de cada perspectiva a una tarea submit (desactivado: ejecución secuencial).
        tareas_pendientes = []
        for n in disparan:
            ventana_p = ventanas[n]
            estado_p = estados_temporales[n]

            if ventana_p.empty:
                logger.warning(f"Ventana vacía para '{n}' en la iteración {iteracion + 1}. No se lanza.")
                continue

            # Restricción de orden de ejecución: las perspectivas consumidoras no se lanzan
            # hasta que TODOS sus productores DECLARADOS hayan producido al menos un modelo.
            # Los productores no declarados se filtran (la consumidora opera autónomamente sobre ellos)
            # comprobando su presencia en `modelos_actuales`, que solo contiene perspectivas activas.
            listado_productores = [
                productor for productor in DEPENDENCIAS_ENTRE_PERSPECTIVAS.get(n, [])
                if productor in modelos_actuales
            ]
            if any(modelos_actuales[productor] is None for productor in listado_productores):
                logger.info(
                    f"La perspectiva '{n}' espera a que sus productores "
                    f"{listado_productores} produzcan modelo. No se lanza en la iteración {iteracion + 1}."
                )
                continue

            # Fusionar la configuración de la perspectiva con la global y su estado temporal propio.
            parametros_dependencias = dependencias.get(n, {})
            config_perspectiva = config_por_nombre[n] | parametros_globales | estado_p | parametros_dependencias

            # Si la perspectiva está configurada para "avance on_trace" y no entró traza nueva,
            # se omite esta iteración para esa perspectiva.
            if config_perspectiva.get('avance') == 'on_trace' and estado_p.get('traza_nueva') is False:
                logger.info(
                    f"No se ha detectado traza nueva para '{n}' en la iteración {iteracion + 1}. "
                    f"No se lanza."
                )
                continue

            # Identificador de la traza más reciente en la ventana de esta perspectiva.
            traza_mas_nueva = obtener_traza_mas_nueva(ventana_p)

            # Submits la tarea lanzar_iteracion para ejecución concurrente con Prefect.
            # TODO: rehabilitar el modo .submit() cuando volvamos a usar Prefect; permite
            # ejecutar las perspectivas que disparan en este tick en paralelo, lo cual es
            # especialmente útil aquí porque el conjunto `disparan` puede contener varias
            # perspectivas cuando sus `fin` coinciden.
            tarea = lanzar_iteracion.submit(config_perspectiva, iteracion, ventana_p, n, cambio_detecado[n], modelos_actuales[n], hist_resultados_perspectivas[n], hist_cambios_perspectivas[n], traza_mas_nueva)
            """
            tarea = lanzar_iteracion(
                config_perspectiva, iteracion, ventana_p, n,
                cambio_detecado[n], modelos_actuales[n],
                hist_resultados_perspectivas[n], hist_cambios_perspectivas[n],
                traza_mas_nueva,
            )
            """
            tareas_pendientes.append(tarea)

        # Esperar resultados de todas las tareas concurrentes.
        # En modo Prefect: tarea.result() bloquea hasta que el future tenga valor.
        resultados_iteracion = [tarea.result() for tarea in tareas_pendientes]
        #resultados_iteracion = [tarea for tarea in tareas_pendientes]

        # Volcar los resultados al estado del orquestador (solo afecta a las perspectivas que
        # dispararon; las demás conservan su `cambio_detecado` previo).
        _procesar_resultados(
            resultados_iteracion, iteracion,
            modelos_actuales, hist_resultados_perspectivas,
            hist_cambios_perspectivas, cambio_detecado,
            dependencias, estados_temporales
        )

        # Autoajuste por perspectiva tras drift confirmado. En multi-ventana
        # cada perspectiva tiene su propio bloque de parámetros, así que el
        # ajuste es independiente y no afecta a las demás.
        for n in disparan:
            if cambio_detecado.get(n, False):
                # Solo ejecutar autoajuste si es la PRIMERA iteración tras el drift.
                # Verificar que el último cambio ocurrió en esta iteración actual.
                hist_cambios = hist_cambios_perspectivas[n]
                if len(hist_cambios) > 0 and hist_cambios[-1]['iteracion'] == iteracion + 1:
                    _aplicar_ajuste_ventana(
                        log_original, n,
                        config_por_nombre[n], parametros_globales,
                        parametros_ventana_por_nombre[n],
                        momento='post-drift',
                        hist_cambios=hist_cambios,
                    )

                    # Burn-in post-drift
                    n_conf_burn = config_por_nombre[n].get('n_confirmacion')
                    if n_conf_burn:
                        # Claves (recurso/par) que confirmaron el drift en las métricas dict,
                        # para marcar el burn-in solo donde el modelo se remina (refit selectivo).
                        recursos_con_cambio = hist_cambios[-1].get('recursos_con_cambio', {})
                        for nombre_metrica, estado_metrica in hist_resultados_perspectivas[n].items():
                            if not isinstance(estado_metrica, dict):
                                continue
                            if 'hist_valores' in estado_metrica:
                                # Métrica escalar: estado plano, se marca directamente.
                                estado_metrica['burn_in'] = int(n_conf_burn)
                            else:
                                # Métrica por-clave (dict anidado clave -> sub-estado): solo las
                                # claves que confirmaron el drift sufren el transitorio del warm-up
                                # (el resto del modelo no se remina), así que solo ellas reciben flag.
                                for clave in recursos_con_cambio.get(nombre_metrica, []):
                                    sub_estado = estado_metrica.get(clave)
                                    if isinstance(sub_estado, dict) and 'hist_valores' in sub_estado:
                                        sub_estado['burn_in'] = int(n_conf_burn)

        iteracion += 1

    return hist_cambios_perspectivas


@flow
def orquestador_multidimensional(config: dict, log: Optional[pd.DataFrame] = None) -> dict:
    """
    Orquestador principal para el análisis de drift utilizando múltiples perspectivas concurrentemente.

    Carga el log, valida la configuración de ventanas y delega en uno de los dos orquestadores
    internos según el modo detectado:
    - 'uni':   todas las perspectivas comparten la ventana definida en configuracion_global.
    - 'multi': cada perspectiva define su propia ventana (todas obligatoriamente temporales).

    Args:
        config: Diccionario de configuración completo que contiene configuraciones globales y por perspectiva.
        log: DataFrame opcional con un log ya cargado (omite la lectura desde disco).
    """

    # Cargar la configuración global
    parametros_globales = config['configuracion_global']

    # Cargar el log
    if log is None:
        ruta_log = parametros_globales['ruta_log']
        extension_log = os.path.splitext(ruta_log)[1].lower()
        if extension_log == '.xes':
            # Algunos logs sintéticos (p. ej. cm-2500 de Maaradji) intercalan dos logs
            # base reutilizando los `case:concept:name`. pm4py preserva las apariciones
            # físicas en el EventLog, pero al convertir a DataFrame las fusiona por id
            # silenciosamente. Asignamos trace_real_index por orden físico de aparición
            # ANTES de convertir, para que cada traza física tenga un identificador único
            # en el DataFrame. El resto del pipeline identifica trazas por trace_real_index.
            event_log = pm4py.read_xes(parametros_globales['ruta_log'], return_legacy_log_object=True)
            for i, trace in enumerate(event_log):
                for event in trace:
                    event['trace_real_index'] = i
            log_original = pm4py.convert_to_dataframe(event_log)
        elif extension_log == '.csv':
            log_original = pd.read_csv(parametros_globales['ruta_log'])
            # Mismo razonamiento que el XES: los CSV pueden tener case ids repetidos
            # entre dos apariciones físicas.
            # Se ordenan, se calcula el trace_real_index por orden de aparición física, y se reordenan por timestamp para el resto del pipeline.
            log_original.sort_values(by='case:concept:name', 
                            key=lambda x: x.str.extract(r'(\d+)', expand=False).astype(int), inplace=True)
            nueva_traza = log_original['case:concept:name'] != log_original['case:concept:name'].shift()
            log_original['trace_real_index'] = nueva_traza.cumsum() - 1
            log_original.sort_values(by='time:timestamp', inplace=True)
        logger.info(f"Log cargado: {parametros_globales['ruta_log']}")
    else:
        # Si el log se proporciona como argumento, se asume que ya tiene un trace_real_index correcto
        log_original = log
        logger.info("Log proporcionado como argumento.")

    # Cargar las perspectivas solicitadas
    perspectivas = config['perspectivas']

    # Validar la configuración de ventanas y decidir el modo de ejecución.
    # Lanza ValueError si las ventanas no son coherentes (multi-ventana exige todas temporales,
    # unidades de duración constante, y rechaza mezclas con ventanas no temporales).
    modo_ventana = validar_ventanas(perspectivas, parametros_globales)
    logger.info(f"Modo de ventanas detectado: '{modo_ventana}'")

    if modo_ventana == 'multi':
        # Cada perspectiva tiene su propia ventana temporal: planificador "fin mínimo".
        return _orquestador_multi_ventana(log_original, parametros_globales, perspectivas)

    # Modo uni: la ventana es la de configuracion_global, o (si por alguna razón no existe)
    # la primera definida en la lista de perspectivas. validar_ventanas ya garantiza que
    # todas son idénticas, así que cualquiera de las dos vale.
    parametros_ventana = parametros_globales.get('ventana') or perspectivas[0].get('ventana')
    return _orquestador_uni_ventana(log_original, parametros_globales, perspectivas, parametros_ventana)
    
# Main
if __name__ == "__main__":

    # Configuración de los parámetros de entrada
    parser = argparse.ArgumentParser(description='Herramienta de minería de procesos multiperspectiva')

    # Fichero de entrada para los parametros
    parser.add_argument('-f', '--file', help='Indica el nombre del fichero donde se almacenan los parámetros de la ejecución',type=str, default=None)

    args = parser.parse_args()

    # Cargar los parámetros del .yml
    parametros = cargar_parametros(args.file)

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

        # Guardar un resumen de los drifts detectados en la capa de reporting.
        ruta_reporte = f"./data/08_reporting/{perspectiva}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.txt"
        with open(ruta_reporte, "w", encoding="utf-8") as f:
            f.write(f"Resumen de drifts - perspectiva: {perspectiva}\n")
            f.write(f"Cambios totales detectados: {len(cambios)}\n\n")
            for cambio in cambios:
                f.write(f"{cambio}\n")
            f.write(f"\nIndices (trace_real_index) detectados: {indices_detectados_exactos}\n")
        logger.info(f"Resumen de drifts guardado en {ruta_reporte}")

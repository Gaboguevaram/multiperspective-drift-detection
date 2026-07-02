import pandas as pd
from scipy.stats import linregress
from typing import Optional, Tuple
from .logging_config import get_logger

logger = get_logger(__name__)

# --- TAREAS DE DETECCIÓN DE CONCEPT DRIFT ---

def identificar_candidato_cambio(ventana_regresion: int, hist_resultados: list[float], hist_candidatos: list[bool], tipo_pendiente_racha: str = None, nombre_metrica: str = None,verbose: bool = False) -> Tuple[list[bool], Optional[str]]:
    """
    Identifica si existe una tendencia significativa (regresión lineal) en las últimas métricas para marcar un candidato a drift.

    Args:
        ventana_regresion: Número de puntos de datos recientes a considerar para la regresión.
        hist_resultados: Historial completo de los valores de la métrica (fitness o precision).
        hist_candidatos: Historial de indicadores booleanos de candidatez.
        tipo_pendiente_racha: Tipo de pendiente que inició la racha.
        nombre_metrica: Nombre de la métrica siendo evaluada.
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

        if verbose:
            logger.debug(f"[{nombre_metrica}] Ventana de regresión: {ventana_regresion}")
            logger.debug(f"[{nombre_metrica}] Últimos {ventana_regresion} valores para regresión: {y}")
            
        # Realizar regresión lineal sobre los últimos 'ventana_regresion' puntos.
        slope, intercept, r_value, p_value, std_err = linregress(x, y)

        if verbose:
            logger.debug(f"[{nombre_metrica}] Pendiente recta regresión: {slope}")
            logger.debug(f"[{nombre_metrica}] P-valor: {p_value}")

        # Reglas:
        # Pendiente negativa (m<)
        m_neg = (slope < 0 and p_value < 0.05)
        # Pendiente positiva (m>)
        m_pos = (slope > 0 and p_value < 0.05)
        # Pendiente neutra
        m_zero = not m_neg and not m_pos

        if verbose:
            logger.debug(f" [{nombre_metrica}] m_neg: {m_neg}, m_pos: {m_pos}, m_zero: {m_zero}")
            logger.debug(f" [{nombre_metrica}] Tipo de pendiente de racha actual: {tipo_pendiente_racha}")

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
                logger.debug(f"   [!] [{nombre_metrica}] Tipo de racha: {tipo_pendiente_racha}, pendiente actual negativa -> RACHA ROTA")
            hist_candidatos.append(False)
            return hist_candidatos, None  # Racha rota, no hay candidato
        if m_pos and tipo_pendiente_racha == 'negativa':
            if verbose:
                logger.debug(f"   [!] [{nombre_metrica}] Tipo de racha: {tipo_pendiente_racha}, pendiente actual positiva -> RACHA ROTA")
            hist_candidatos.append(False)
            return hist_candidatos, None  # Racha rota, no hay candidato
        
        #TODO: Ahora mismo, una cancelación de racha no supone el inicio de una nueva
        # Solucionable en confirmar cambio??, guardar el tipo de pendiente que inicio la racha y q sea el mismo

        if es_candidato:
            logger.warning(f"   [!] [{nombre_metrica}] Ventana marcada como CANDIDATA a drift (Pendiente: {slope:.4f}, p-value: {p_value:.4f})")
            razon = 'Pendiente negativa' if m_neg else 'Pendiente positiva' if m_pos else 'Pendiente neutra con candidato previo'
            logger.warning(f"Razón: {razon}")
            tipo_pendiente_racha = 'negativa' if m_neg else 'positiva' if m_pos else 'neutra'

    else:
        if verbose:
            logger.debug(f"[{nombre_metrica}] No hay suficientes datos para identificar candidato a drift (tamaño historial: {len(hist_resultados)})")

    hist_candidatos.append(es_candidato)

    return hist_candidatos, tipo_pendiente_racha

def confirmar_cambio(ventana_confirmacion: int, nombre_metrica: str, historial_metrica: list[bool], verbose: bool = False) -> bool:
    """
    Confirma el Concept Drift si una cantidad suficiente de las últimas ventanas marcadas como candidatas pertenecen a la misma métrica.

    Args:
        ventana_confirmacion: Número de confirmaciones consecutivas necesarias.
        nombre_metrica: Nombre de la métrica.
        historial_metrica: Lista de candidatos para la métrica.
        verbose: Nivel de detalle en los mensajes de registro.

    Returns:
        True si se confirma un drift, False en caso contrario.
    """

    drift_confirmado = False

    if verbose:
        logger.debug(f"Ventana de confirmación: {ventana_confirmacion}")

    if len(historial_metrica) < ventana_confirmacion:
        if verbose:
            logger.debug(f"No hay suficientes datos para confirmar drift en {nombre_metrica} (tamaño historial: {len(historial_metrica)})")
        return False  # No hay suficientes datos para confirmar
    
    ultimos_candidatos = historial_metrica[-ventana_confirmacion:]

    if verbose:
        logger.debug(f"Últimos candidatos para {nombre_metrica}: {ultimos_candidatos}")

    if all(ultimos_candidatos):
        drift_confirmado = True
        logger.critical(f"   [!!!] ({nombre_metrica}) DRIFT CONFIRMADO. Se procederá a recalcular el modelo.")
    
    return drift_confirmado


def obtener_traza_mas_nueva(ventana: pd.DataFrame) -> tuple:
    """
    Devuelve trace_real_index de la traza que entró
    más recientemente en la ventana (la de timestamp de inicio más tardío).
    Las trazas son siempre completas por filtrado previo.
    """
    # Agrupamos por trace_real_index (único por aparición física) en lugar de
    # case:concept:name, que puede repetirse en logs sintéticos intercalados.
    primer_evento_por_traza = (
        ventana.sort_values('time:timestamp')
               .groupby('trace_real_index', sort=False)['time:timestamp']
               .min()
    )
    # La traza más nueva es la que empezó más tarde; idxmax ya devuelve el trace_real_index.
    return primer_evento_por_traza.idxmax()

def deteccion_concept_drift_regresion(parametros: dict, nombre_metrica: str, valor_metrica: float, estado_metrica: dict, traza_mas_nueva: int) -> Tuple[bool, dict, Optional[int]]:
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

    DEBUG = parametros.get('debug', False)

    # Inicializar listas históricas si no existen
    estado_metrica.setdefault('hist_valores', [])
    estado_metrica.setdefault('hist_candidatos', [])

    # Burn-in post-drift
    if estado_metrica.get('burn_in', 0) > 0:
        estado_metrica['burn_in'] -= 1
        logger.info(
            f"[{nombre_metrica}] Burn-in post-drift: {estado_metrica['burn_in']} iteraciones "
            f"restantes; detección omitida (warm-up del modelo reminado)."
        )
        return False, estado_metrica, None
    
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
        nombre_metrica=nombre_metrica,
        verbose=DEBUG
    )

    # ¿La iteración actual es candidata?
    es_candidato_ahora = estado_metrica['hist_candidatos'][-1] if estado_metrica['hist_candidatos'] else False
    logger.info(f"[{nombre_metrica}] Marcada como candidata a drift: {es_candidato_ahora}")

    # Si no es candidata y no hay razón de cambio, resetear tipo de pendiente
    if not es_candidato_ahora and razon_cambio is None:
        tipo_pendiente_racha = None

    # Iteración anterior fue candidata
    hubo_candidato_previo = (
        len(estado_metrica['hist_candidatos']) > 1 and 
        estado_metrica['hist_candidatos'][-2]
    )
    logger.info(f"[{nombre_metrica}] Iteración anterior fue candidata: {hubo_candidato_previo}")

    # Inicio de racha nueva: guardar τ_i solo una vez
    if es_candidato_ahora and not hubo_candidato_previo:
        tau_primer_candidato = traza_mas_nueva
        tipo_pendiente_racha = razon_cambio
        logger.debug(f"[{nombre_metrica}] [→] Racha iniciada en traza {tau_primer_candidato}")

    # Racha rota sin confirmación: limpiar
    if not es_candidato_ahora:
        tau_primer_candidato = None

    # Confirmar el drift basado en la ventana de confirmación
    drift_confirmado = confirmar_cambio(parametros['n_confirmacion'], nombre_metrica, estado_metrica['hist_candidatos'], verbose=DEBUG)

    if drift_confirmado and DEBUG:
        # Traza de diagnóstico: valores de la métrica en la ventana de confirmación.
        valores_conf = estado_metrica['hist_valores'][-parametros['n_confirmacion']:]
        rango_conf = (max(valores_conf) - min(valores_conf)) if len(valores_conf) >= 2 else 0.0
        logger.debug(
            f"[{nombre_metrica}] Valores de la ventana de confirmación (rango={rango_conf:.4f}): {valores_conf}"
        )

    traza_drift = None
    if drift_confirmado:
        # Obtener la traza más antigua de la última iteración (que causó la confirmación)
        traza_drift = tau_primer_candidato
        logger.warning(f"[{nombre_metrica}] Drift confirmado en traza: {traza_drift}")
        
    # Guardar estado para la siguiente iteración
    estado_metrica['tau_primer_candidato'] = tau_primer_candidato
    estado_metrica['tipo_pendiente_racha'] = tipo_pendiente_racha

    return drift_confirmado, estado_metrica, traza_drift


def deteccion_concept_drift_distribucion(parametros: dict, nombre_metrica: str, valor_metrica: bool, estado_metrica: dict, traza_mas_nueva: int) -> Tuple[bool, dict, Optional[int]]:
    """
    Detección de Concept Drift basada en pertenencia a la distribución de referencia,
    aplicada a un par (recurso, tarea) concreto.

    El orquestador itera el dict {par -> bool} producido por
    `comparar_pertenencia_a_distribucion` y llama a esta función UNA VEZ POR PAR,
    pasando como `nombre_metrica` un identificador propio del par (de modo que
    `estado_metrica` mantiene historial y traza de racha independientes por par)
    y como `valor_metrica` el bool "candidata a drift en esta ventana" del par.

    La confirmación se hace con la racha estándar: si los últimos `n_confirmacion`
    valores del historial son todos True, se declara drift y se devuelve la traza
    en la que comenzó la racha como `traza_drift`.

    Args:
        parametros: Diccionario de configuración para el drift.
        nombre_metrica: Identificador del par (recurso, tarea) en esta iteración.
        valor_metrica: True si la ventana actual es candidata a drift para el par.
        estado_metrica: Diccionario con el estado histórico del par específico.
        traza_mas_nueva: Índice de la traza más reciente.

    Returns:
        Tupla (drift_confirmado, estado_actualizado, traza_drift).
    """

    DEBUG = parametros.get('debug', False)

    # Inicializar listas históricas si no existen
    estado_metrica.setdefault('hist_valores', [])
    
    # Traza del primer candidato de la racha activa (None si no hay racha)
    tau_primer_candidato = estado_metrica.get('tau_primer_candidato', None)
   
    # Registrar el valor actual
    estado_metrica['hist_valores'].append(valor_metrica)

    # ¿La iteración actual es candidata?
    es_candidato_ahora = estado_metrica['hist_valores'][-1] if estado_metrica['hist_valores'] else False
    logger.info(f"[{nombre_metrica}] Marcada como candidata a drift: {es_candidato_ahora}")

    # Iteración anterior fue candidata
    hubo_candidato_previo = (
        len(estado_metrica['hist_valores']) > 1 and 
        estado_metrica['hist_valores'][-2]
    )

    logger.info(f"[{nombre_metrica}] Iteración anterior fue candidata: {hubo_candidato_previo}")

    # Inicio de racha nueva: guardar τ_i solo una vez
    if es_candidato_ahora and not hubo_candidato_previo:
        tau_primer_candidato = traza_mas_nueva
        logger.info(f"[{nombre_metrica}] [→] Racha iniciada en traza {tau_primer_candidato}")

    # Racha rota sin confirmación: limpiar
    if not es_candidato_ahora:
        tau_primer_candidato = None

    # Confirmar el drift basado en la ventana de confirmación
    drift_confirmado = confirmar_cambio(parametros['n_confirmacion'], nombre_metrica, estado_metrica['hist_valores'], verbose=DEBUG)
    
    traza_drift = None
    if drift_confirmado:
        # Obtener la traza más antigua de la última iteración (que causó la confirmación)
        traza_drift = tau_primer_candidato
        logger.warning(f"[{nombre_metrica}] Drift confirmado en traza: {traza_drift}")
        
    # Guardar estado para la siguiente iteración
    estado_metrica['tau_primer_candidato'] = tau_primer_candidato

    return drift_confirmado, estado_metrica, traza_drift
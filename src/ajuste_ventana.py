from __future__ import annotations

import numpy as np
import pandas as pd
import pm4py
from scipy import stats
from typing import Optional, Tuple, Union

from .ventana import extraccion_ventana, avanzar_ventana
from .logging_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Cálculo automático de salto_ventana: el desplazamiento equivalente a una
# traza nueva en cada tipo de ventana. Se invoca SOLO en el ajuste inicial.
# ---------------------------------------------------------------------------

def calcular_salto_equivalente_traza(
    log: pd.DataFrame,
    tipo_ventana: str,
) -> Optional[Union[int, pd.Timedelta]]:
    """
    Calcula el `salto_ventana` cuyo desplazamiento equivale, en promedio, a la
    incorporación de una nueva traza a la ventana del orquestador.

    Esta unidad común permite que `n_confirmacion = tamano_ventana / salto_ventana`
    tenga el mismo significado en los tres tipos de ventana ("cuántas trazas hacen
    falta para confirmar drift"), liberando al usuario de tener que afinar a mano un
    `salto_ventana` distinto para cada tipo de log.

    Reglas por tipo de ventana:
    - 'trazas':   1 (cada iteración avanza exactamente una traza, por definición).
    - 'eventos':  promedio de eventos por traza = len(log) / nº trazas, redondeado y
                  clampado a un mínimo de 1.
    - 'temporal': mediana del inter-arrival time entre los primeros eventos de trazas
                  consecutivas. Se usa mediana (no media) para reducir el sesgo de
                  outliers (huecos largos sin trazas son comunes en logs reales).

    Devuelve None si no se puede calcular (log vacío, una sola traza en 'temporal',
    tipo desconocido).
    """
    # Contamos trazas físicas por trace_real_index (único por aparición física)
    # en lugar de case:concept:name, que puede repetirse en logs sintéticos intercalados.
    total_trazas = log['trace_real_index'].nunique() if 'trace_real_index' in log.columns else 0
    if total_trazas == 0:
        return None

    if tipo_ventana == 'trazas':
        return 1

    if tipo_ventana == 'eventos':
        return max(1, int(round(len(log) / total_trazas)))

    if tipo_ventana == 'temporal':
        # Necesitamos al menos dos trazas para definir un inter-arrival.
        if total_trazas < 2:
            return None
        # Timestamp del primer evento de cada traza física, ordenado cronológicamente.
        primeros_eventos = (
            log.sort_values('time:timestamp')
            .groupby('trace_real_index', sort=False)['time:timestamp']
            .min()
            .sort_values()
        )
        diferencias = primeros_eventos.diff().dropna()
        if diferencias.empty:
            return None
        return pd.Timedelta(diferencias.median())

    return None


# ---------------------------------------------------------------------------
# Recalibrado de n_confirmacion y n_regresion en función del cociente
# tamano_ventana / salto_ventana. Se invoca tanto en el ajuste inicial como
# en el post-drift, justo después de actualizar tamano_ventana.
# ---------------------------------------------------------------------------

def derivar_n_confirmacion_regresion(parametros_ventana: dict) -> Optional[Tuple[int, int]]:
    """Calcula (n_confirmacion, n_regresion) a partir de tamano_ventana y salto_ventana.

    En este orquestador el detector recibe un punto del historial por cada iteración,
    no por cada traza. Por tanto, una "ventana completa" del log se traduce en
    `tamano_ventana / salto_ventana` iteraciones — que es el `n` del Algoritmo 3.2:

        n_confirmacion = tamano_ventana / salto_ventana   (iteraciones)
        n_regresion    = n_confirmacion // 2

    Para ventanas temporales el cociente se calcula como ratio de pd.Timedelta;
    para ventanas por eventos/trazas se hace división convencional.

    Devuelve None si no se puede calcular (parámetros faltantes, división inválida,
    o tipos incompatibles).
    """
    tamano = parametros_ventana.get('tamano_ventana')
    salto = parametros_ventana.get('salto_ventana')
    if tamano is None or salto is None:
        return None

    try:
        if parametros_ventana.get('tipo') == 'temporal':
            ratio = pd.Timedelta(tamano) / pd.Timedelta(salto)
        else:
            ratio = float(tamano) / float(salto)
    except (ZeroDivisionError, TypeError, ValueError):
        return None

    if not np.isfinite(ratio) or ratio <= 0:
        return None

    n_confirmacion = max(1, int(round(ratio)))
    n_regresion = max(1, n_confirmacion // 2)
    return n_confirmacion, n_regresion

_UMBRALES_MODELO = {
    'inductive_miner':       0.0,
    'heuristic_miner':       0.0
}

_MODELOS_SOPORTADOS = set(modelo for modelo in _UMBRALES_MODELO)


# ---------------------------------------------------------------------------
# Punto de entrada: convierte un trace_real_index a la posición inicial
# correcta según el tipo de ventana de la perspectiva.
# ---------------------------------------------------------------------------

def posicion_inicial_desde_traza(
    log: pd.DataFrame,
    trace_real_index: int,
    tipo_ventana: str,
) -> Optional[Union[int, pd.Timestamp]]:
    """Convierte trace_real_index a la posición compatible con el tipo de
    ventana ('temporal' → pd.Timestamp, 'trazas'/'eventos' → int).
    Devuelve None si la traza no existe en el log."""
    fila = log[log['trace_real_index'] == trace_real_index]
    if fila.empty:
        logger.warning(f"trace_real_index={trace_real_index} no encontrado en el log.")
        return None

    # Identificamos la traza física por trace_real_index (no por case:concept:name,
    # que puede repetirse en logs sintéticos intercalados).
    eventos_traza = log[log['trace_real_index'] == trace_real_index]
    primer_ts = pd.Timestamp(eventos_traza['time:timestamp'].min())

    if tipo_ventana == 'temporal':
        return primer_ts

    # Lista de trazas físicas ordenadas por primer evento (necesaria para ambos tipos restantes).
    trazas_ordenadas = (
        log.sort_values('time:timestamp')
        .groupby('trace_real_index', sort=False)['time:timestamp']
        .min().sort_values().index.tolist()
    )

    if tipo_ventana == 'trazas':
        try:
            return trazas_ordenadas.index(trace_real_index)
        except ValueError:
            logger.warning(f"trace_real_index={trace_real_index} no encontrado en trazas_ordenadas.")
            return None

    if tipo_ventana == 'eventos':
        log_ord = log.sort_values('time:timestamp').reset_index(drop=True)
        idx = log_ord.index[log_ord['trace_real_index'] == trace_real_index].tolist()
        return int(idx[0]) if idx else None

    raise ValueError(f"Tipo de ventana no soportado: '{tipo_ventana}'")


# ---------------------------------------------------------------------------
# Algoritmo principal de ajuste de ventana
# ---------------------------------------------------------------------------

def ajustar_tamano_ventana(
    log: pd.DataFrame,
    posicion_inicial: Union[int, pd.Timestamp],
    config_perspectiva: dict,
    parametros_ventana: dict,
) -> Optional[Union[int, pd.Timedelta]]:
    """Busca el menor tamaño de ventana n tal que tres sub-ventanas no solapadas
    consecutivas desde posicion_inicial sean NO equivalentes para la perspectiva.

    Devuelve el nuevo tamano_ventana en las unidades del tipo de ventana
    ('temporal' → pd.Timedelta, 'eventos'/'trazas' → int), o None si no se
    puede aplicar el ajuste.
    """
    tipo = parametros_ventana['tipo']
    nombre_modelo = config_perspectiva.get('modelo')
    nombre_perspectiva = config_perspectiva.get('nombre', '?')

    if nombre_modelo not in _MODELOS_SOPORTADOS:
        logger.info(
            f"[ajuste_ventana] '{nombre_perspectiva}': modelo '{nombre_modelo}' "
            f"sin comparador registrado. Se omite el ajuste."
        )
        return None

    # Extraer parámetros de ajuste para evitar múltiples .get()
    ajuste_n_min = parametros_ventana.get('ajuste_n_min')
    ajuste_delta = parametros_ventana.get('ajuste_delta')
    ajuste_n_max = parametros_ventana.get('ajuste_n_max')

    # Calcular n_min, delta y n_max según el tipo de ventana.
    # Defaults alineados con la tesis: 1 %, 0.1 % y 50 % del log respectivamente.
    if tipo == 'temporal':
        ts_col = pd.to_datetime(log['time:timestamp'], utc=True, errors='coerce').dropna()
        span = (ts_col.max() - ts_col.min()) if not ts_col.empty else pd.Timedelta(hours=1)
        n_min = pd.Timedelta(ajuste_n_min) if ajuste_n_min else max(span * 0.01, pd.Timedelta(hours=1))
        delta = pd.Timedelta(ajuste_delta) if ajuste_delta else max(span * 0.001, pd.Timedelta(minutes=10))
        n_max = pd.Timedelta(ajuste_n_max) if ajuste_n_max else span * 0.5
    elif tipo == 'trazas':
        # Contamos trazas físicas por trace_real_index
        total = max(log['trace_real_index'].nunique(), 1)
        n_min = int(ajuste_n_min or max(1, (total * 15) // 1000))  # 1.5 % del log, mínimo 1
        #n_min = int(ajuste_n_min or max(1, total // 100))
        delta = int(ajuste_delta or max(1, total // 1000))
        n_max = int(ajuste_n_max or max(1, total // 2))
    else:  # eventos
        total = max(len(log), 1)
        n_min = int(ajuste_n_min or max(1, total // 100))
        delta = int(ajuste_delta or max(1, total // 1000))
        n_max = int(ajuste_n_max or max(1, total // 2))

    n = n_min

    for _ in range(1000):  # límite de seguridad
        if n > n_max:
            logger.info(f"[ajuste_ventana] '{nombre_perspectiva}': alcanzado n_max={n_max}.")
            return n_max

        # Construir parámetros temporales donde tamano = salto = n
        # para que extraccion_ventana y avanzar_ventana generen tres bloques
        # consecutivos no solapados a partir de posicion_inicial.
        temp_params = {**parametros_ventana, 'tamano_ventana': n, 'salto_ventana': n}
        if tipo == 'temporal':
            temp_params['fecha_inicial'] = posicion_inicial
        elif tipo == 'eventos':
            temp_params['primer_evento'] = int(posicion_inicial)
        else:
            temp_params['primera_traza'] = int(posicion_inicial)

        try:
            sub1, estado1 = extraccion_ventana(log, config_perspectiva, temp_params)
            sub2, estado2 = avanzar_ventana(log, sub1, config_perspectiva, temp_params, estado1)
            sub3, _       = avanzar_ventana(log, sub2, config_perspectiva, temp_params, estado2)
        except Exception as exc:
            logger.debug(f"[ajuste_ventana] '{nombre_perspectiva}' error con n={n}: {exc}")
            n = n + delta
            continue

        if any(s is None or len(s) == 0 for s in (sub1, sub2, sub3)):
            n = n + delta
            continue

        if _tres_subventanas_son_iguales(sub1, sub2, sub3, nombre_modelo):
            n = n + delta
            continue

        logger.info(
            f"[ajuste_ventana] '{nombre_perspectiva}': nuevo tamano_ventana={n} "
            f"(posicion_inicial={posicion_inicial})."
        )
        return n

    logger.warning(
        f"[ajuste_ventana] '{nombre_perspectiva}': iteraciones agotadas. Devolviendo n_max={n_max}."
    )
    return n_max


# ---------------------------------------------------------------------------
# Comparador de equivalencia: toda la lógica de firma en una sola función
# ---------------------------------------------------------------------------

def _tres_subventanas_son_iguales(
    sub1: pd.DataFrame,
    sub2: pd.DataFrame,
    sub3: pd.DataFrame,
    nombre_modelo: str
) -> bool:
    """Decide si tres sub-ventanas son equivalentes para una perspectiva.
    Cada rama engloba la construcción de la firma y la comparación para ese
    tipo de modelo. Devuelve True (equivalentes → crecer n) o False (difieren
    → n es el tamaño adecuado)."""
    sublogs = (sub1, sub2, sub3)

    if nombre_modelo in ('inductive_miner', 'heuristic_miner'):
        # Comparamos el OLP del play-out de la red.
        olps = []
        for s in sublogs:
            try:
                if nombre_modelo == 'inductive_miner':
                    net, im, fm = pm4py.discover_petri_net_inductive(s)
                else:
                    net, im, fm = pm4py.discover_petri_net_heuristics(s)
                log_simulado = pm4py.play_out(net, im, fm)
                dfg_modelo, _, _ = pm4py.discover_dfg(log_simulado)
                olps.append(frozenset(dfg_modelo.keys()))
            except Exception:
                return False
        return olps[0] == olps[1] == olps[2]
    else:
        # Resto de perspectivas sin ajuste
        logger.warning(
            f"No existe comparador registrado para modelo '{nombre_modelo}'. "
            f"Se utilizará el tamaño de ventana predeterminado."
        )
        return False

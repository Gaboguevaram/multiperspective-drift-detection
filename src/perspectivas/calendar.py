import pandas as pd
import numpy as np

from ..logging_config import get_logger
from pix_framework.io.event_log import read_csv_log
from pix_framework.io.event_log import DEFAULT_XES_IDS
from pix_framework.discovery.resource_calendar_and_performance.fuzzy.discovery import EventLogIDs, discovery_fuzzy_resource_calendars_and_performances


# Mapeo entre nomenclatura XES (con ':' en las columnas) e identificadores que espera
# pix-framework. pix-framework usa itertuples() internamente y los ':' de XES no son
# atributos válidos, por lo que renombramos *solo* en los puntos de contacto con
# pix-framework. El resto del pipeline opera con nomenclatura XES (DEFAULT_XES_IDS).
_PIX_IDS = EventLogIDs()
_XES_A_PIX = {
    DEFAULT_XES_IDS.case: _PIX_IDS.case,
    DEFAULT_XES_IDS.activity: _PIX_IDS.activity,
    DEFAULT_XES_IDS.start_time: _PIX_IDS.start_time,
    DEFAULT_XES_IDS.end_time: _PIX_IDS.end_time,
    DEFAULT_XES_IDS.enabled_time: _PIX_IDS.enabled_time,
    DEFAULT_XES_IDS.resource: _PIX_IDS.resource,
}
_PIX_A_XES = {v: k for k, v in _XES_A_PIX.items()}

logger = get_logger(__name__)

########################
# ----- CALENDAR ----- #
########################

def filtrado_calendarios(log: pd.DataFrame, parametros: dict):
    """
    Devuelve un log filtrado en nomenclatura XES. El renombrado a identificadores
    válidos se hace solo dentro de los puntos de contacto con pix-framework (ver
    modelo_calendarios).
    """

    # Seleccionar columnas necesarias en nomenclatura XES.
    columnas_necesarias = [DEFAULT_XES_IDS.case, DEFAULT_XES_IDS.activity, DEFAULT_XES_IDS.start_time, DEFAULT_XES_IDS.end_time, DEFAULT_XES_IDS.enabled_time, DEFAULT_XES_IDS.resource]

    log_filtrado = log[columnas_necesarias].copy()

    DEBUG = parametros.get('debug', False)

    if DEBUG:
        log_filtrado.to_csv(f"./data/02_intermediate/calendar_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_filtrado.csv", index=False)

    return log_filtrado

def transformacion_calendarios(log: pd.DataFrame, parametros: dict):
    """
    Normaliza tipos y orden del log (timestamps a UTC, recurso a str, rellenar NaN, ordenar).
    Opera sobre nomenclatura XES — el renombrado a identificadores válidos para pix-framework
    se hace solo en modelo_calendarios, justo antes de la llamada al descubridor.
    """

    DEBUG = parametros.get('debug', False)

    log = log.rename(columns=_XES_A_PIX)

    log_ids = EventLogIDs()
    missing_resource = "NOT_SET"
    sort = True

    # Basado en la implementación de pix-framework (Apache License 2.0)
    # Fuente: https://github.com/AutomatedProcessImprovement/pix-framework/blob/main/src/pix_framework/io/event_log.py

    event_log = log

    # Set case id as object
    event_log = event_log.astype({log_ids.case: object})
    # Fix missing resources (don't do it if [missing_resources] is set to None)
    if missing_resource:
        if log_ids.resource not in event_log.columns:
            event_log[log_ids.resource] = missing_resource
        else:
            event_log[log_ids.resource] = event_log[log_ids.resource].fillna(missing_resource)
    # Set resource type to string if numeric
    if log_ids.resource in event_log.columns:
        event_log[log_ids.resource] = event_log[log_ids.resource].apply(str)
    # Convert timestamp value to pd.Timestamp (setting timezone to UTC)
    event_log[log_ids.end_time] = pd.to_datetime(event_log[log_ids.end_time], utc=True, format="ISO8601")
    if log_ids.start_time in event_log.columns:
        event_log[log_ids.start_time] = pd.to_datetime(event_log[log_ids.start_time], utc=True, format="ISO8601")
    if log_ids.enabled_time in event_log.columns:
        event_log[log_ids.enabled_time] = pd.to_datetime(event_log[log_ids.enabled_time], utc=True, format="ISO8601")
    # Sort by end time
    if sort:
        if log_ids.start_time in event_log.columns and log_ids.enabled_time in event_log.columns:
            event_log = event_log.sort_values([log_ids.start_time, log_ids.end_time, log_ids.enabled_time])
        elif log_ids.start_time in event_log.columns:
            event_log = event_log.sort_values([log_ids.start_time, log_ids.end_time])
        else:
            event_log = event_log.sort_values(log_ids.end_time)

    event_log = pd.DataFrame(event_log)

    if DEBUG:
        event_log.to_csv(f"./data/03_primary/calendar_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_transformado.csv", index=False)

    return event_log.rename(columns=_PIX_A_XES)

def _dump_calendarios_csv(resource_calendar, ruta: str) -> None:
    """
    Serializa los intervalos de los calendarios fuzzy a CSV para depuración.
    """
    filas = []
    for rc in resource_calendar:
        for interval in rc.intervals:
            filas.append({
                'from_day': interval.from_day,
                'to_day': interval.to_day,
                'start_time': interval._start_time,
                '_end_time': interval._end_time,
                'probability': interval.probability,
                'resource_name': rc.resource_name,
            })
    if not filas:
        return
    df_calendarios = pd.DataFrame(filas)
    df_calendarios['_end_time'] = df_calendarios['_end_time'].astype(str).str.replace('00:00:00', '23:59:59')
    df_calendarios.to_csv(ruta, index=False)


def modelo_calendarios(log: pd.DataFrame, parametros: dict) -> dict:
    """
    Descubre calendarios de recursos sobre el log.

    Si ``parametros`` contiene las claves internas ``_recursos_a_redescubrir`` (lista de
    nombres de recurso) y ``_modelo_anterior`` (dict con el modelo previo), realiza un
    descubrimiento parcial: solo redescubre los calendarios de los recursos indicados y
    los fusiona con los del modelo anterior. Los calendarios de recursos no afectados
    se conservan inalterados; los recursos solicitados que ya no aparecen en el log se
    eliminan. NO se incorporan recursos nuevos: los recursos del log que no estuvieran
    ya en el modelo anterior quedan fuera por construcción del filtro previo al
    discovery. Esta limitación es deliberada (ver project_refit_parcial_no_descubre_nuevos).

    Si no se reciben esas claves, hace descubrimiento completo (comportamiento original).
    """

    DEBUG = parametros.get('debug', False)
    recursos_a_redescubrir = parametros.get('_recursos_a_redescubrir')
    modelo_anterior = parametros.get('_modelo_anterior')

    redescubrimiento_parcial = bool(recursos_a_redescubrir) and modelo_anterior is not None

    if redescubrimiento_parcial:
        logger.info(f"Redescubriendo calendarios solo para recursos: {recursos_a_redescubrir}")
        log_subconjunto = log[log[DEFAULT_XES_IDS.resource].isin(recursos_a_redescubrir)]
        if log_subconjunto.empty:
            logger.warning(
                "El log filtrado para redescubrimiento parcial está vacío. "
                "Se conserva el modelo anterior eliminando los recursos solicitados."
            )
            modelo_filtrado = [rc for rc in modelo_anterior['modelo']
                               if rc.resource_name not in recursos_a_redescubrir]
            recursos_finales = [rc.resource_name for rc in modelo_filtrado]
            #TODO: objective es eliminable
            return {'modelo': modelo_filtrado, 'recursos': recursos_finales, 'objective': 'calendar'}
        log_para_descubrir = log_subconjunto
    else:
        logger.info("Descubriendo calendarios de recursos (descubrimiento completo)")
        log_para_descubrir = log

    # XES -> esquema pix-framework (necesario para itertuples() dentro de pix-framework)
    log_pix = log_para_descubrir.rename(columns=_XES_A_PIX)

    # Granularidad fija a 60 minutos
    nuevos_calendarios, _ = discovery_fuzzy_resource_calendars_and_performances(
        log_pix,
        log_ids=_PIX_IDS,
        granularity=60,
    )

    if redescubrimiento_parcial:
        # Mezclar: conservar recursos no afectados y sustituir los redescubiertos.
        # No se incorporan recursos del log que no estuvieran ya en el modelo: el filtro
        # `log_subconjunto = log[...].isin(recursos_a_redescubrir)` aplicado más arriba
        # impide por construcción que el discovery vea nada fuera de `recursos_a_redescubrir`,
        # y `recursos_a_redescubrir` siempre es subconjunto de los recursos del modelo
        # anterior.
        nuevos_por_nombre = {rc.resource_name: rc for rc in nuevos_calendarios}
        modelo_combinado = []

        for rc in modelo_anterior['modelo']:
            if rc.resource_name in recursos_a_redescubrir:
                logger.info(f"Redescubriendo recurso '{rc.resource_name}' marcado para redescubrimiento parcial.")
                # Sustituir si el redescubrimiento devolvió uno; si no, se elimina.
                if rc.resource_name in nuevos_por_nombre:
                    modelo_combinado.append(nuevos_por_nombre[rc.resource_name])
            else:
                logger.info(f"Conservando recurso '{rc.resource_name}' no marcado para redescubrimiento.")
                modelo_combinado.append(rc)

        resource_calendar = modelo_combinado
    else:
        # TODO: creo que sobra el list() porque discovery_fuzzy_resource_calendars_and_performances ya devuelve lista, pero lo dejo por claridad.
        resource_calendar = list(nuevos_calendarios)

    recursos = [rc.resource_name for rc in resource_calendar]
    
    # Guardar siempre el calendario descubierto en la capa de modelos.
    nombre_archivo = f"./data/06_models/Calendar_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.csv"
    logger.info(f"Guardando el calendario descubierto como {nombre_archivo}")
    _dump_calendarios_csv(resource_calendar, nombre_archivo)

    return {'modelo': resource_calendar, 'recursos': recursos, 'objective': 'calendar'}
import pandas as pd
import numpy as np

from ..logging_config import get_logger
from pix_framework.io.event_log import read_csv_log
from pix_framework.io.event_log import DEFAULT_XES_IDS, EventLogIDs
from pix_framework.discovery.resource_calendar_and_performance.fuzzy.discovery import discovery_fuzzy_resource_calendars_and_performances
from pix_framework.discovery.resource_calendar_and_performance.crisp.discovery import discover_crisp_resource_calendars_per_profile
from pix_framework.discovery.resource_calendar_and_performance.crisp.resource_calendar import CalendarKPIInfoFactory


logger = get_logger(__name__)

########################
# ----- CALENDAR ----- #
########################

# TODO: inicialmente se exigue el start time para la perspectiva, se puede parametrizar a futuro

def filtrado_calendarios(log: pd.DataFrame, parametros: dict):
    """
    Devuelve un log filtrado listo para ser usado por pix-framework.
    """

    # Seleccionar columnas necesarias
    columnas_necesarias = [DEFAULT_XES_IDS.case, DEFAULT_XES_IDS.activity, DEFAULT_XES_IDS.start_time, DEFAULT_XES_IDS.end_time, DEFAULT_XES_IDS.enabled_time, DEFAULT_XES_IDS.resource]
    
    # Crear copia del log con las columnas seleccionadas
    log_filtrado = log[columnas_necesarias].copy()
    
    valid_ids = EventLogIDs()
    rename_map = {
        DEFAULT_XES_IDS.case: valid_ids.case,
        DEFAULT_XES_IDS.activity: valid_ids.activity,
        DEFAULT_XES_IDS.start_time: valid_ids.start_time,
        DEFAULT_XES_IDS.end_time: valid_ids.end_time,
        DEFAULT_XES_IDS.enabled_time: valid_ids.enabled_time,
        DEFAULT_XES_IDS.resource: valid_ids.resource
    }
    log_filtrado.rename(columns=rename_map, inplace=True)

    DEBUG = parametros['debug']

    if DEBUG:
        log_filtrado.to_csv(f"./logs/calendar_filtrado.csv", index=False)

    return log_filtrado

def transformacion_calendarios(log: pd.DataFrame, parametros: dict):
    """
    Transforma el log filtrado para que sea compatible con pix-framework.
    """

    DEBUG = parametros['debug']

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
        event_log.to_csv(f"./logs/calendar_transformado.csv", index=False)

    return event_log

def modelo_calendarios(log: pd.DataFrame, parametros: dict) -> dict:
    """
    """

    DEBUG = parametros['debug']

    resource_calendar, activity_resource_distribution = discovery_fuzzy_resource_calendars_and_performances(log, log_ids=EventLogIDs())

    calendarios = []
    recursos =  []
    for rc in resource_calendar:
        resource_name = rc.resource_name
        intervals = rc.intervals
        for interval in intervals:
            calendario = {
                'from_day': interval.from_day,
                'to_day': interval.to_day,
                'start_time': interval._start_time,
                '_end_time' : interval._end_time,
                'probability': interval.probability,
                'resource_name': resource_name
            }
            calendarios.append(calendario)
        recursos.append(resource_name)
    
    df_calendarios = pd.DataFrame(calendarios)

    if DEBUG:
        df_calendarios.to_csv(f"./logs/calendarios_modelo.csv", index=False)

    #calendarios = discover_crisp_resource_calendars_per_profile(log, log_ids=EventLogIDs(),)

    return {'modelo': resource_calendar, 'recursos': recursos, 'objective': 'calendar'}
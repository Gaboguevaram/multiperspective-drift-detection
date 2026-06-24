from src.perspectivas.calendar import filtrado_calendarios, modelo_calendarios, transformacion_calendarios

from .perspectivas.control_flow import *
from .perspectivas.arrival_rate import *
from .perspectivas.service_rate import *
from .ventana import *
from .metricas import *
from .concept_drift_detection import *

##############################
# --- REGISTRO DE OPCIONES ---
##############################

REGISTRO_FILTRADO = {
    "filtrar_trazas_completas": filtrar_trazas_completas,
    "filtrado_arrival_rate": filtrado_arrival_rate,
    "filtrado_service_rate": filtrado_service_rate,
    "filtrado_calendarios": filtrado_calendarios
}

REGISTRO_TRANSFORMACIONES = {
    "transformacion_simple": transformacion_simple,
    "transformacion_arrival_rate": transformacion_arrival_rate,
    "transformacion_service_rate": transformacion_service_rate,
    "transformacion_calendarios": transformacion_calendarios,
    "calcular_hora_fin": None,
    "agrupar_por_evento": None
}

REGISTRO_MODELOS = {
    "inductive_miner": inductive_miner,
    "heuristic_miner": heuristic_miner,
    "modelo_arrival_rate": modelo_arrival_rate,
    "modelo_service_rate": modelo_service_rate,
     "modelo_calendarios": modelo_calendarios,
    "distribuciones_temporales": None,
}

REGISTRO_METRICAS = {
    "fitness": calcular_fitness,
    "precision": calcular_precision,
    "MSE": calcular_metrica_modelo_sklearn,
    "MAE": calcular_metrica_modelo_sklearn,
    "soporte": calcular_support
}

REGISTRO_DETECCION = {
    "deteccion_regresion": deteccion_concept_drift
}

METRICAS_VALIDACION_MODELO = {
    "MAE" : "neg_mean_absolute_error",
    "MSE" : "neg_mean_squared_error"
}
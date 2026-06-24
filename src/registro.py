from .perspectivas.calendar import *
from .perspectivas.resource_profiles import *
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
    "filtrado_calendarios": filtrado_calendarios,
    "filtrado_resource_colab": filter_resource_colab,
    "filtrado_resource_productivity": filter_resource_productivity,
    "filtrado_resource_skill": filter_resource_skill,
    "filtrado_resource_utilization": filter_resource_utilization
}

REGISTRO_TRANSFORMACIONES = {
    "transformacion_simple": transformacion_simple,
    "transformacion_arrival_rate": transformacion_arrival_rate,
    "transformacion_service_rate": transformacion_service_rate,
    "transformacion_calendarios": transformacion_calendarios,
    "transformacion_resource_productivity": transformacion_resource_productivity,
    "transformacion_resource_utilization": transformacion_resource_utilization,
}

REGISTRO_MODELOS = {
    "inductive_miner": inductive_miner,
    "heuristic_miner": heuristic_miner,
    "modelo_arrival_rate": modelo_arrival_rate,
    "modelo_service_rate": modelo_service_rate,
    "modelo_calendarios": modelo_calendarios,
    "modelo_resource_productivity": modelo_resource_productivity,
}

REGISTRO_METRICAS = {
    "fitness": calcular_fitness,
    "precision": calcular_precision,
    "MSE": calcular_metrica_modelo_sklearn,
    "MAE": calcular_metrica_modelo_sklearn,
    "soporte": calcular_support,
    "soporte_invertido": calcular_inverted_support,
    "resource_colab": resource_colab,
    "resource_skill": resource_skill,
    "resource_utilization": resource_utilization,
    "comparar_pertenencia_a_distribucion": comparar_pertenencia_a_distribucion,
    "comparar_distribuciones": comparar_distribuciones
}

REGISTRO_DETECCION = {
    "deteccion_regresion": deteccion_concept_drift_regresion,
    "deteccion_distribucion": deteccion_concept_drift_distribucion,
}

METRICAS_VALIDACION_MODELO = {
    "MAE" : "neg_mean_absolute_error",
    "MSE" : "neg_mean_squared_error"
}

##################################
# --- REGISTRO DE DEPENDENCIAS ---
##################################

# Disposición consumidor-productor:
# dependencias[consumidor][productor]

DEPENDENCIAS_ENTRE_PERSPECTIVAS = {
    "resource_productivity" : ["calendarios"],
    "resource_utilization" : ["calendarios"]
}
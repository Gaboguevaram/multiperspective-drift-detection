import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from pix_framework.io.event_log import DEFAULT_XES_IDS
from ..logging_config import get_logger

logger = get_logger(__name__)

########################
# --- ARRIVAL-RATE --- #
########################

def filtrado_arrival_rate(log: pd.DataFrame, parametros: dict):
    """
    Devuelve un log filtrado que solo contiene los eventos correspondientes al inicio de una traza.
    """
    DEBUG = parametros.get('debug', False)

    logger.info("Filtrando log para cálculo de arrival rate (solo eventos de inicio de traza)")
    
    # Se usa la tarea inicial del proceso para filtrar

    if DEBUG:
        logger.debug(f"Tarea de inicio para filtrado: {parametros['primera_tarea']}")

    log_filtrado = log[log[DEFAULT_XES_IDS.activity] == parametros['primera_tarea']].copy()

    # Eliminar la zona horaria (por conveniencia en el cálculo de arrival rate)

    # Se convierte a datetime para asegurarnos de que es del tipo correcto
    log_filtrado[DEFAULT_XES_IDS.end_time] = pd.to_datetime(log_filtrado[DEFAULT_XES_IDS.end_time])

    # Se eliminan las zonas horarias para evitar problemas en el cálculo de diferencias de tiempo
    log_filtrado[DEFAULT_XES_IDS.end_time] = log_filtrado[DEFAULT_XES_IDS.end_time].dt.tz_localize(None)

    # Eliminar los microsegundos para evitar problemas de precisión en el cálculo de arrival rate
    log_filtrado[DEFAULT_XES_IDS.end_time] = log_filtrado[DEFAULT_XES_IDS.end_time].dt.floor('s')

    log_filtrado = log_filtrado[[DEFAULT_XES_IDS.case, DEFAULT_XES_IDS.end_time, 'trace_real_index', DEFAULT_XES_IDS.activity]]

    if DEBUG:
        log_filtrado.to_csv(f"./data/02_intermediate/arrival_rate_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_filtrado.csv", index=False)

    if log_filtrado.empty:
        logger.warning("El log filtrado para arrival rate está vacío. No han llegado tareas nuevas para esta ventana")
        
    return log_filtrado

def transformacion_arrival_rate(log: pd.DataFrame, parametros: dict):
    """
    Calcula la tasa de llegada (arrival rate) agrupando eventos por período de tiempo.
    """

    DEBUG = parametros.get('debug', False)

    logger.info("Transformando log para cálculo de arrival rate")

    granularidad_arrival_rate = parametros['granularidad_arrival_rate']
    frecuencia_arrival_rate = parametros['frecuencia_arrival_rate']

    if DEBUG:
        logger.debug(f"Granularidad para arrival rate: {granularidad_arrival_rate}")
        logger.debug(f"Frecuencia para arrival rate: {frecuencia_arrival_rate}")

    fecha_inicial = pd.Timestamp(parametros['inicio']).tz_localize(None)
    fecha_final = fecha_inicial + pd.Timedelta(granularidad_arrival_rate)

    # Se obtiene la fecha final del log del final de la ventana
    ultima_fecha_log = pd.Timestamp(parametros['fin']).tz_localize(None)

    if DEBUG:
        logger.debug(f"Fecha inicial del log: {fecha_inicial}")
        logger.debug(f"Fecha final del log: {ultima_fecha_log}")

    resultados = []
    iter = 0
    
    while True:

        # Condición de salida
        if fecha_final >= ultima_fecha_log:
            break

        ventana_arrival_rate = log[(log['time:timestamp'] >= pd.Timestamp(fecha_inicial)) & (log['time:timestamp'] < pd.Timestamp(fecha_final))].copy()

        arrival_rate = ventana_arrival_rate.shape[0]

        #logger.debug(f"Arrival rate en esta ventana: {arrival_rate} eventos")
        
        resultados.append({
            'time:timestamp': fecha_final,
            'arrival_rate': arrival_rate
        })

        #if DEBUG:
        #    ventana_arrival_rate.to_csv(f"./logs/arrival_rate_ventana_{iter}.csv", index=False)
        
        fecha_inicial = fecha_inicial + pd.Timedelta(frecuencia_arrival_rate)
        fecha_final = fecha_inicial + pd.Timedelta(granularidad_arrival_rate)
        iter += 1

    log_transformado = pd.DataFrame(resultados)

    # Extraemos componentes temporales para enriquecer el modelo
    log_transformado['year'] = log_transformado['time:timestamp'].dt.year
    log_transformado['month'] = log_transformado['time:timestamp'].dt.month
    log_transformado['day'] = log_transformado['time:timestamp'].dt.day
    log_transformado['hour'] = log_transformado['time:timestamp'].dt.hour
    log_transformado['minute'] = log_transformado['time:timestamp'].dt.minute
    log_transformado['second'] = log_transformado['time:timestamp'].dt.second
    log_transformado['week_day'] = log_transformado['time:timestamp'].dt.dayofweek
    log_transformado['es_fin_de_semana'] = log_transformado['week_day'] >= 5

    log_transformado = log_transformado.sort_values('time:timestamp').reset_index(drop=True)

    if DEBUG:
        log_transformado.to_csv(f"./data/03_primary/arrival_rate_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_transformado.csv", index=False)

    return log_transformado

def _calcular_periodos_frecuencias(frecuencia_str: str, temporalidades: list[str]) -> dict[str, int]:

    frecuencia_base = int(frecuencia_str.split(' ')[0])
    temporalidad = frecuencia_str.split(' ')[1]

    if temporalidad not in temporalidades:
        raise ValueError(f"Temporalidad desconocida: {temporalidad}. Las opciones válidas son: {temporalidades}")
    
    # Convertir a segundos para facilitar el cálculo de periodos
    if temporalidad == 'second':
        pass
    elif temporalidad == 'min':
        frecuencia_base *= 60
    elif temporalidad == 'hour':
        frecuencia_base *= 3600
    elif temporalidad == 'day':
        frecuencia_base *= 3600 * 24
    elif temporalidad == 'week':
        frecuencia_base *= 3600 * 24 * 7
    elif temporalidad == 'month':
        frecuencia_base *= 3600 * 24 * 7 * 4 
    
    # Cálculo de frecuencias
    frecuencias = {}
    for temporalidad in temporalidades:
        if temporalidad == 'second':
            frecuencias['second'] = frecuencia_base
        elif temporalidad == 'min':
            frecuencias['min'] = frecuencia_base / 60
        elif temporalidad == 'hour':
            frecuencias['hour'] = frecuencia_base / 3600
        elif temporalidad == 'day':
            frecuencias['day'] = frecuencia_base / (3600 * 24)
        elif temporalidad == 'week':
            frecuencias['week'] = frecuencia_base / (3600 * 24 * 7)
        elif temporalidad == 'month':
            frecuencias['month'] = frecuencia_base / (3600 * 24 * 7 * 4)


    periodos = {}
    # Cálculo de períodos
    for temporalidad in temporalidades:
        if temporalidad == 'min':
            periodos['min'] = float(60 / frecuencia_base)
        elif temporalidad == 'hour':
            periodos['hour'] = float(60 * periodos.get('min'))  # 60 minutos en una hora, dividido por la frecuencia de arrival rate (ej. 15 min -> 4 periodos de 15 min en una hora)
        elif temporalidad == 'day':
            periodos['day'] = float(24 * periodos.get('hour'))
        elif temporalidad == 'week':
            periodos['week'] = float(7 * periodos.get('day'))
        elif temporalidad == 'month':
            periodos['month'] = float(12 * periodos.get('day'))

    # Convertir el período a entero
    periodos = {k: int(v) for k, v in periodos.items()}

    return periodos, frecuencias

def preprocesado_arrival_rate(log: pd.DataFrame, parametros: dict, verbose: bool, test: bool, features: list = None) -> pd.DataFrame:
    """
    Preprocesa el log para el cálculo del arrival rate. Esto incluye la eliminación de columnas irrelevantes y la selección de características con varianza no nula.
    """

    DEBUG = parametros.get('debug', False)

    # Columnas que no aportan información
    # El año no es relevante, pues no es algo estacional
    columnas_a_eliminar = ['case:concept:name', 'trace_real_index', 'year']

    log_preprocesado = log.copy()

    ##################################################
    # Codificar las variables temporales en períodos #
    ##################################################

    # Obtener las temporalidades superiores a la frecuencia actual
    frecuencia_arrival_rate = parametros['frecuencia_arrival_rate']

    temporalidades = ['second', 'min', 'hour', 'day', 'week', 'month']
    periodos, frecuencias = _calcular_periodos_frecuencias(frecuencia_arrival_rate, temporalidades)

    log_preprocesado[f'intervalo_minuto'] = log_preprocesado['second'] // frecuencias['second']
    log_preprocesado[f'intervalo_hora'] = log_preprocesado['minute'] // frecuencias['min'] + log_preprocesado['intervalo_minuto']
    log_preprocesado[f'intervalo_diario'] = log_preprocesado['hour'] // frecuencias['hour'] + log_preprocesado['intervalo_hora']
    log_preprocesado[f'intervalo_semanal'] = log_preprocesado['week_day'] // frecuencias['day'] + log_preprocesado['intervalo_diario']
    log_preprocesado[f'intervalo_mensual'] = log_preprocesado['month'] // frecuencias['week'] + log_preprocesado['intervalo_semanal']
  
    ####################################################################
    # Aplicación de codificación ciclíca para las variables temporales #
    ####################################################################

    if verbose:
        logger.info("Períodos calculados para codificación cíclica:")
        for temporalidad, periodo in periodos.items():
            logger.info(f"  {temporalidad}: {periodo}")

    for tupla in periodos.items():

        temporalidad, periodo = tupla
        
        if periodo <= 1:
            logger.warning(f"El período calculado para la temporalidad {temporalidad} es {periodo}, lo cual no es válido para la codificación cíclica. Se omitirá esta temporalidad.")
            continue

        if temporalidad == 'min':
            log_preprocesado['Min_sin'] = np.sin(log_preprocesado['intervalo_minuto'] * (2 * np.pi / periodo))
            log_preprocesado['Min_cos'] = np.cos(log_preprocesado['intervalo_minuto'] * (2 * np.pi / periodo))
        if temporalidad == 'hour':
            log_preprocesado['Hour_sin'] = np.sin(log_preprocesado['intervalo_hora'] * (2 * np.pi / periodo))
            log_preprocesado['Hour_cos'] = np.cos(log_preprocesado['intervalo_hora'] * (2 * np.pi / periodo))
        elif temporalidad == 'day':
            log_preprocesado['Day_sin'] = np.sin(log_preprocesado['intervalo_diario'] * (2 * np.pi / periodo))
            log_preprocesado['Day_cos'] = np.cos(log_preprocesado['intervalo_diario'] * (2 * np.pi / periodo))
        elif temporalidad == 'week':
            log_preprocesado['Week_sin'] = np.sin(log_preprocesado['intervalo_semanal'] * (2 * np.pi / periodo))
            log_preprocesado['Week_cos'] = np.cos(log_preprocesado['intervalo_semanal'] * (2 * np.pi / periodo))
        elif temporalidad == 'month':
            log_preprocesado['Month_sin'] = np.sin(log_preprocesado['intervalo_mensual'] * (2 * np.pi / periodo))
            log_preprocesado['Month_cos'] = np.cos(log_preprocesado['intervalo_mensual'] * (2 * np.pi / periodo))

    columnas_a_eliminar += ['time:timestamp', 'second', 'minute', 'day', 'hour','month','week_day', 'intervalo_minuto', 'intervalo_hora', 'intervalo_diario', 'intervalo_semanal', 'intervalo_mensual']

    # En los logs usados (días/semanas) la columna `month` tiene 1-2 valores únicos, así
    # que Month_sin/Month_cos quedan casi constantes: aportan poca señal y son una excusa
    # para sobreajustar. Week_* y los lags ya cubren la estacionalidad útil.
    columnas_a_eliminar += ['Month_sin', 'Month_cos']

    ###########################
    # Adición de lag features #
    ###########################

    # Agregar features de lag de las 3 filas anteriores
    for i in range(1, 4):
        log_preprocesado[f'arrival_rate_lag_{i}'] = log_preprocesado['arrival_rate'].shift(i)

    if not test:
        # Se eliminan las filas con valores NaN (son solo 3)
        log_preprocesado.dropna(inplace=True)

    # Agregar una feature por temporalidad superior
    for temporalidad in temporalidades[1:]:  # Empezamos desde la segunda, ya que la primera es 'second', que no tiene sentido como temporalidad superior
        if periodos[temporalidad] <= 1:
            logger.warning(f"El período calculado para la temporalidad {temporalidad} es {periodos[temporalidad]}, lo cual no es válido para la creación de features de lag. Se omitirá esta temporalidad.")
            continue
        log_preprocesado[f'arrival_rate_lag_{periodos[temporalidad]}'] = log_preprocesado['arrival_rate'].shift(periodos[temporalidad])

    # Eliminar las columnas originales que ya no son necesarias para el modelo
    log_preprocesado = log_preprocesado.drop(columns=columnas_a_eliminar, errors='ignore')

    if DEBUG:
        log_preprocesado.to_csv(f"./data/05_model_input/arrival_rate_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_model_input.csv", index=False)

    return log_preprocesado

def modelo_arrival_rate(log: pd.DataFrame, parametros: dict, random_state: int = 42) -> dict:
    """
    Entrena un modelo de regresión para predecir el arrival rate.
    """

    DEBUG = parametros.get('debug', False)

    logger.info("Descubriendo Modelo de Arrival Rate")

    # Entrenar el modelo usando los datos de arrival rate
    modelo = RandomForestRegressor(random_state=random_state)

    log_preprocesado = preprocesado_arrival_rate(log, parametros, verbose=DEBUG, test=False)

    # Separar características y variable objetivo
    train_X = log_preprocesado.drop(columns=["arrival_rate"])
    train_Y = log_preprocesado["arrival_rate"]

    if DEBUG:
        train_X.to_csv(f"./data/04_feature/arrival_rate_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}_caracteristicas.csv", index=False)

    if DEBUG:
        logger.debug(f"Datos de entrenamiento (train_X): {train_X}")
        logger.debug(f"Datos de la variable objetivo de entrenamiento (train_Y): {train_Y}")

    # Grid de hiperparámetros a probar
    param_grid = {
        "max_depth": [20, 50, None],
        "min_samples_split": range(2, 20, 2),
        "min_samples_leaf": range(1, 9),
        "n_estimators": [100, 200, 300]
    }
    
    # Método de división TimeSeriesSplit para validación cruzada
    tscv = TimeSeriesSplit(n_splits=3)

    # Random Search
    # Resolver scoring: el usuario indica "MAE" o "MSE" en el YAML y se traduce al
    # scorer de sklearn vía el mapping centralizado de registro.py.
    from ..registro import METRICAS_VALIDACION_MODELO
    metrica_yaml = parametros.get('metrica_validacion_modelo', 'MAE')
    scoring_sklearn = METRICAS_VALIDACION_MODELO.get(metrica_yaml, metrica_yaml)


    grid = RandomizedSearchCV(
        estimator=modelo,
        param_distributions=param_grid,
        cv=tscv,
        scoring=scoring_sklearn,
        n_jobs=1,
        n_iter=80,  # Número de combinaciones aleatorias a probar
        random_state=random_state
    )

    logger.info("Realizando Randomized Search para optimización de hiperparámetros...")

    grid.fit(train_X, train_Y)

    logger.info(f"Mejores parámetros: {grid.best_params_}")
    logger.info(f"Mejor resultado: {abs(grid.best_score_):.4f} ({metrica_yaml})")

    modelo = grid.best_estimator_

    # Guardar siempre el modelo entrenado en la capa de modelos.
    ruta_modelo = f"./data/06_models/ArrivalRate_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.joblib"
    joblib.dump(modelo, ruta_modelo)
    logger.info(f"Modelo de arrival rate guardado en {ruta_modelo}")

    return {'modelo': modelo, 'features': train_X.columns.tolist(), 'objective': 'arrival_rate'}
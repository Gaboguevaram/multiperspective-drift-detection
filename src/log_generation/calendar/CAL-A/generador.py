import argparse
import subprocess
from pathlib import Path
import pandas as pd
import os

# Identificador del caso. Solo cambia este valor entre Caso_A / Caso_B / Caso_C / Caso_D.
CASO = "CAL_A"

# Ruta de destino donde se guardarán los logs finales:
# <raíz>/data/01_raw/calendar/<CASO>/
# parents[4] sube desde Caso_X/generador_no_drift.py hasta la raíz del repo
# (Caso_X -> calendar -> log_generation -> src -> raíz).
RAIZ      = Path(__file__).resolve().parents[4]
DESTINO   = RAIZ / "data" / "01_raw" / "calendar" / CASO
DESTINO.mkdir(parents=True, exist_ok=True)

def generar_log_secuencial(TOTAL_CASES: int = 2500, TIPO_CAMBIO: str = "recurring"):
    # ==========================================================================
    # PASO 1: DEFINICIÓN DE PARÁMETROS
    # ==========================================================================

    if TIPO_CAMBIO not in ["recurring", "sudden"]:
        raise ValueError(f"Tipo de cambio no reconocido: {TIPO_CAMBIO}. Debe ser 'recurring' o 'sudden'.")

    if TIPO_CAMBIO == "recurring":
        CHUNK_SIZE = int(0.1 * TOTAL_CASES)  # 10% de los casos por iteración
    elif TIPO_CAMBIO == "sudden":
        CHUNK_SIZE = int(0.5 * TOTAL_CASES)  # 50% de los casos por iteración

    NUM_ITERATIONS = TOTAL_CASES // CHUNK_SIZE  
    
    # Fecha de inicio inicial para la iteración 0
    fecha_inicio_actual = "2026-04-15T12:30:00.000Z"
    
    lista_dataframes = []
    
    print(f"Iniciando simulación secuencial: {TOTAL_CASES} casos en {NUM_ITERATIONS} bloques de {CHUNK_SIZE}.")

    # ==========================================================================
    # PASO 2: BUCLE DE SIMULACIÓN (FLUJO LÓGICO)
    # ==========================================================================
    for i in range(NUM_ITERATIONS):
        # PASO 3: ALTERNAR CONFIGURACIONES
        # Iteraciones pares (0, 2, 4) usan 'base', impares (1, 3) usan 'drift'
        config_file = "prosimos_base.json" if i % 2 == 0 else "prosimos_drift.json"
        output_temp = f"temp_chunk_{i}.csv"
        
        print(f"\n--- Iteración {i} (Config: {config_file}) ---")
        
        # PASO 4 y 5: CONSTRUIR Y LANZAR COMANDO PROSIMOS
        # Usamos la fecha_inicio_actual que se actualiza en cada ciclo
        comando = (
            f'prosimos start-simulation '
            f'--bpmn_path "./modelo.bpmn" '
            f'--json_path "./{config_file}" '
            f'--total_cases {CHUNK_SIZE} '
            f'--starting_at "{fecha_inicio_actual}" '
            f'--log_out_path "./{output_temp}"'
        )
        
        print(f"Ejecutando: {comando}")
        subprocess.run(comando, shell=True, check=True)

        # ==========================================================================
        # PASO 6: EXTRACCIÓN Y FORMATEO DE LA FECHA (EDUCATIVO)
        # ==========================================================================
        # Leemos el CSV recién generado para obtener el momento exacto en que terminó
        df_chunk = pd.read_csv(output_temp)
        
        # Convertimos la columna 'end_time' a objeto datetime de Pandas
        # Prosimos suele devolver fechas que Pandas reconoce bien, pero forzamos el formato
        tiempos_fin = pd.to_datetime(df_chunk['end_time'], format='mixed')
        
        # Buscamos la fecha máxima (el último evento del último caso)
        max_end_time = tiempos_fin.max()
        
        # FORMATEO CRUCIAL: La terminal de Prosimos requiere ISO 8601 estricto (YYYY-MM-DDTHH:MM:SS.mmmZ)
        # .strftime con microsegundos truncados a milisegundos y la Z de UTC
        fecha_inicio_actual = max_end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        print(f"Próxima simulación empezará en: {fecha_inicio_actual}")
        
        # Guardamos el DataFrame y limpiamos el archivo temporal
        lista_dataframes.append(df_chunk)
        if os.path.exists(output_temp):
            os.remove(output_temp)

    # ==========================================================================
    # PASO 7: CONCATENACIÓN
    # ==========================================================================
    df_final = pd.concat(lista_dataframes, ignore_index=True)

    # ==========================================================================
    # PASO 8: REMAPEO MATEMÁTICO DE IDs (EDUCATIVO)
    # ==========================================================================
    # Prosimos reinicia el case_id a 0 en cada ejecución de subprocess.
    # Para que el log final sea coherente, debemos desplazar los IDs de cada chunk.
    # Como cada chunk tiene CHUNK_SIZE casos, el chunk 'i' debe empezar sus IDs en 'i * CHUNK_SIZE'.
    
    print("\nRemapeando IDs de casos para asegurar secuencialidad...")
    
    def remapear_ids(row, size):
        # La posición del chunk se puede inferir del índice original si usamos ignore_index=False,
        # pero es más seguro calcularlo por bloques en la concatenación.
        # Aquí aplicamos una lógica simple: dado que concatenamos en orden, 
        # recalculamos el case_id basándonos en su aparición.
        pass # Usaremos una lógica más directa sobre el DF final

    # Lógica directa: Identificamos cada grupo de 250 casos y les sumamos su base
    # Pero es más fácil: como sabemos que cada chunk tiene exactamente CHUNK_SIZE casos únicos
    # y vienen ordenados, podemos usar una transformación basada en el número de fila / eventos por caso,
    # o simplemente llevar un contador de cambios de ID original.
    
    # Versión robusta: Crear un nuevo ID basado en la iteración y el ID original de Prosimos
    ids_corregidos = []
    for iter_idx, df in enumerate(lista_dataframes):
        # A cada case_id de este chunk le sumamos el desplazamiento (iteración * tamaño)
        df['case_id'] = df['case_id'] + (iter_idx * CHUNK_SIZE)
    
    # Re-concatenamos con los IDs ya corregidos
    df_final = pd.concat(lista_dataframes, ignore_index=True)

    # ==========================================================================
    # PASO 9: FORMATO XES Y EXPORTACIÓN
    # ==========================================================================
    # Renombrar columnas al estándar de Process Mining (XES)
    df_final.rename(columns={
        'case_id': 'case:concept:name',
        'activity': 'concept:name',
        'enable_time': 'time:enabled',
        'start_time': 'start_timestamp',
        'end_time': 'time:timestamp',
        'resource': 'org:resource'
    }, inplace=True)

    # Convertir a datetime para ordenar correctamente
    df_final['time:timestamp'] = pd.to_datetime(df_final['time:timestamp'], format='mixed')
    
    # Ordenar por tiempo (fundamental para detección de drift)
    df_final = df_final.sort_values(by='time:timestamp')

    df_final['case:concept:name'] = df_final['case:concept:name'].apply(lambda x: f'case[{x}]')

    # Guardar resultado final
    output_file = DESTINO / f"./Rc_{TIPO_CAMBIO}_{CASO.lower()}_{TOTAL_CASES}-{TIPO_CAMBIO}.csv"
    df_final.to_csv(output_file, index=True)
    
    print(f"\n¡Éxito! Archivo generado: {output_file}")
    print(f"Total de eventos: {len(df_final)}")
    print(f"Rango de IDs: {df_final['case:concept:name'].min()} a {df_final['case:concept:name'].max()}")

if __name__ == "__main__":

    # Configuración de los parámetros de entrada
    parser = argparse.ArgumentParser()

    # Tipo de cambio
    parser.add_argument('-t', '--tipo', help='Tipo de cambio',type=str, default="recurring")

    modos = ["recurring", "sudden"]

    args = parser.parse_args()

    tamanos = [2500, 5000, 7500, 10000]
    for tamano in tamanos:
        for modo in modos:
            print(f"\nGenerando log para cambio {modo}...")
            generar_log_secuencial(tamano, modo)

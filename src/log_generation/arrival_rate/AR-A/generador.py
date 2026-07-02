import argparse
import subprocess
from pathlib import Path
import pandas as pd
import os

# Identificador del caso. 
CASO = "AR_A"

# Ruta de destino donde se guardarán los logs finales
RAIZ      = Path(__file__).resolve().parents[4]
DESTINO   = RAIZ / "data" / "01_raw" / "arrival_rate" / CASO
DESTINO.mkdir(parents=True, exist_ok=True)

def generar_log_secuencial(TOTAL_CASES: int = 2500, TIPO_CAMBIO: str = "sudden"):

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

    for i in range(NUM_ITERATIONS):
        # Iteraciones pares (0, 2, 4) usan 'base', impares (1, 3) usan 'drift'
        config_file = "prosimos_base.json" if i % 2 == 0 else "prosimos_drift.json"
        output_temp = f"temp_chunk_{i}.csv"

        print(f"\n--- Iteración {i} (Config: {config_file}) ---")

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

    df_final = pd.concat(lista_dataframes, ignore_index=True)

    # Prosimos reinicia el case_id a 0 en cada ejecución de subprocess.
    # Para que el log final sea coherente, debemos desplazar los IDs de cada chunk.
    # Como cada chunk tiene CHUNK_SIZE casos, el chunk 'i' debe empezar sus IDs en 'i * CHUNK_SIZE'.

    print("\nRemapeando IDs de casos para asegurar secuencialidad...")

    for iter_idx, df in enumerate(lista_dataframes):
        # A cada case_id de este chunk le sumamos el desplazamiento (iteración * tamaño)
        df['case_id'] = df['case_id'] + (iter_idx * CHUNK_SIZE)

    # Re-concatenamos con los IDs ya corregidos
    df_final = pd.concat(lista_dataframes, ignore_index=True)

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

    # Guardar resultado final. Nombre: <CODIGO>_<tipo>_<casos>.csv (incluye 'sudden' para
    # que el motor de validación fije el ground truth en el 50%).
    output_file = DESTINO / f"./{CASO}_{TIPO_CAMBIO}_{TOTAL_CASES}.csv"
    df_final.to_csv(output_file, index=True)

    print(f"\n¡Éxito! Archivo generado: {output_file}")
    print(f"Total de eventos: {len(df_final)}")
    print(f"Rango de IDs: {df_final['case:concept:name'].min()} a {df_final['case:concept:name'].max()}")

if __name__ == "__main__":

    # Configuración de los parámetros de entrada
    parser = argparse.ArgumentParser()

    # Tipo de cambio
    parser.add_argument('-t', '--tipo', help='Tipo de cambio', type=str, default="sudden")

    args = parser.parse_args()

    tamanos = [5000]
    for tamano in tamanos:
        print(f"\nGenerando log para cambio {args.tipo}...")
        generar_log_secuencial(tamano, args.tipo)

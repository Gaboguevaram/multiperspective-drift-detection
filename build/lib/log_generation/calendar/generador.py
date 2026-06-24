import subprocess
import pandas as pd
import argparse

def crear_log_con_arrival_rate(TOTAL_CASES_PER_LOG=1250):
    # ==========================================
    # PARÁMETROS DEL EXPERIMENTO
    # ==========================================
    CHUNK_SIZE = (TOTAL_CASES_PER_LOG * 2) // 10

    # ==========================================
    # PASO 1: EJECUTAR LAS SIMULACIONES
    # ==========================================
    print("Iniciando Simulación 1")
    comando_base = f'prosimos start-simulation --bpmn_path "./modelo.bpmn" --json_path "./prosimos_base.json" --total_cases {TOTAL_CASES_PER_LOG} --starting_at "2026-04-15T12:30:00.000Z" --log_out_path "./log_base.csv"'
    subprocess.run(comando_base, shell=True)

    print("Iniciando Simulación 2")
    comando_drift = f'prosimos start-simulation --bpmn_path "./modelo.bpmn" --json_path "./prosimos_drift.json" --total_cases {TOTAL_CASES_PER_LOG} --starting_at "2026-04-15T12:30:00.000Z" --log_out_path "./log_drift.csv"'
    subprocess.run(comando_drift, shell=True)

    # ==========================================
    # PASO 2: CARGAR Y PREPARAR DATOS
    # ==========================================
    print("Procesando logs en Pandas...")
    df_base = pd.read_csv("./log_base.csv")
    df_drift = pd.read_csv("./log_drift.csv")

    columna_inicio = 'start_time'
    columna_fin = 'end_time'
    enable_time = 'enable_time'

    for df in [df_base, df_drift]:
        df[columna_inicio] = pd.to_datetime(df[columna_inicio], format='mixed')
        df[columna_fin] = pd.to_datetime(df[columna_fin], format='mixed')
        df[enable_time] = pd.to_datetime(df[enable_time], format='mixed')

    # ==========================================
    # PASO 3 Y 4: INTERCALAR, ARREGLAR IDs Y TIEMPOS
    # ==========================================
    print("Aplicando Recurring Concept Drift (Intercalando chunks)...")

    chunks_procesados = []
    ultimo_end_time = None
    case_id_global = 0 # Llevaremos un contador global para que los IDs vayan de 0 a 2499 sin repetirse

    # Calculamos cuántas iteraciones necesitamos (1250 / 250 = 5 ciclos)
    num_iteraciones = TOTAL_CASES_PER_LOG // CHUNK_SIZE

    for i in range(num_iteraciones):
        # 1. Definir los límites del caso para este chunk (ej. 0 a 249, luego 250 a 499...)
        inicio_idx = i * CHUNK_SIZE
        fin_idx = (i + 1) * CHUNK_SIZE

        # 2. Extraer los fragmentos correspondientes de cada dataset original
        chunk_base = df_base[(df_base['case_id'] >= inicio_idx) & (df_base['case_id'] < fin_idx)].copy()
        chunk_drift = df_drift[(df_drift['case_id'] >= inicio_idx) & (df_drift['case_id'] < fin_idx)].copy()

        # Vamos a procesar siempre en el mismo orden: primero la base, luego el drift
        for chunk in [chunk_base, chunk_drift]:
            if chunk.empty:
                continue
                
            # --- A) SOLUCIÓN DE IDs ---
            # Extraemos los IDs únicos originales de este fragmento y los mapeamos a nuevos IDs consecutivos
            ids_originales = chunk['case_id'].unique()
            mapa_ids = {viejo_id: (case_id_global + j) for j, viejo_id in enumerate(ids_originales)}
            chunk['case_id'] = chunk['case_id'].map(mapa_ids)
            
            # Aumentamos el contador global de IDs para el siguiente fragmento
            case_id_global += len(ids_originales)

            # --- B) SOLUCIÓN DE TIEMPOS ---
            # Si no es el primer chunk de la historia, calculamos el salto temporal
            if ultimo_end_time is not None:
                primer_start_chunk = chunk[columna_inicio].min()
                delta_tiempo = ultimo_end_time - primer_start_chunk
                
                # Empujamos todo el fragmento hacia el futuro
                chunk[columna_inicio] = chunk[columna_inicio] + delta_tiempo
                chunk[columna_fin] = chunk[columna_fin] + delta_tiempo
                chunk[enable_time] = chunk[enable_time] + delta_tiempo

            # Guardamos cuándo termina este fragmento para que el siguiente sepa dónde empezar
            ultimo_end_time = chunk[columna_fin].max()

            # Añadimos el fragmento arreglado a nuestra lista final
            chunks_procesados.append(chunk)

    # ==========================================
    # PASO 5: LA FUSIÓN FINAL
    # ==========================================
    df_final = pd.concat(chunks_procesados, ignore_index=True)

    # Formateo estándar XES para minería de procesos
    df_final['case_id'] = df_final['case_id'].apply(lambda x: f'case[{x}]')
    # Quitar micro segundos
    #df_final['time:timestamp'] = df_final['time:timestamp'].dt.floor('S')
    #df_final['time:end_timestamp'] = df_final['time:end_timestamp'].dt.floor('S')

    df_final.rename(columns={
        'case_id': 'case:concept:name',
        'activity': 'concept:name',
        'enable_time': 'time:enabled',
        'start_time': 'start_timestamp',
        'end_time': 'time:timestamp',
        'resource' : 'org:resource'
    }, inplace=True)

    df_final = df_final[['case:concept:name', 'concept:name', 'time:enabled', 'start_timestamp', 'time:timestamp', 'org:resource']]

    # Ordenamiento cronológico crucial para los algoritmos de detección
    df_final = df_final.sort_values(by=['time:timestamp']).reset_index(drop=True)

    total_trazas = df_final['case:concept:name'].nunique()

    df_final.to_csv(f"./log_calendar_{total_trazas}.csv", index=True)
    print(f"¡Éxito! Log con Drift Recurrente generado. Total de trazas: {total_trazas}")

if __name__ == "__main__":

    tamanos = [1250, 2500, 3750, 5000]

    for tamano in tamanos:
        crear_log_con_arrival_rate(TOTAL_CASES_PER_LOG=tamano)
import subprocess
import pandas as pd
from pathlib import Path

# Identificador del caso.
CASO = "Caso_D"

# Ruta de destino donde se guardarán los logs finales.
RAIZ      = Path(__file__).resolve().parents[4]
DESTINO   = RAIZ / "data" / "01_raw" / "resource_productivity" / CASO
DESTINO.mkdir(parents=True, exist_ok=True)


def generar_log_secuencial(TOTAL_CASES: int = 2500):

    output_temp        = "log.csv"
    config_file        = "prosimos_base.json"
    fecha_inicio_actual = "2026-04-15T12:30:00.000Z"

    print(f"Iniciando simulación secuencial: {TOTAL_CASES} casos para {CASO}")

    comando = (
        f'prosimos start-simulation '
        f'--bpmn_path "./modelo.bpmn" '
        f'--json_path "./{config_file}" '
        f'--total_cases {TOTAL_CASES} '
        f'--starting_at "{fecha_inicio_actual}" '
        f'--log_out_path "./{output_temp}"'
    )

    print(f"Ejecutando: {comando}")
    subprocess.run(comando, shell=True, check=True)

    df_final = pd.read_csv(output_temp)

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

    # Guardar resultado final en data/01_raw/resource_productivity/<CASO>/
    output_file = DESTINO / f"log_productivity_{CASO.lower()}_{TOTAL_CASES}.csv"
    df_final.to_csv(output_file, index=True)

    print(f"\n¡Éxito! Archivo generado: {output_file}")
    print(f"Total de eventos: {len(df_final)}")
    print(f"Rango de IDs: {df_final['case:concept:name'].min()} a {df_final['case:concept:name'].max()}")

if __name__ == "__main__":
    tamanos = [2500, 5000]
    for tamano in tamanos:
        generar_log_secuencial(tamano)

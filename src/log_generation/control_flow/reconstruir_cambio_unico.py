import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pm4py

RAIZ = Path(__file__).resolve().parents[3]
ORIGEN = RAIZ / "data" / "01_raw" / "control_flow"
DESTINO = RAIZ / "data" / "01_raw" / "control_flow" / "single"
DESTINO.mkdir(parents=True, exist_ok=True)

# Número de bloques en que el benchmark de Maaradji divide cada log (9 cambios -> 10 bloques).
NUM_BLOQUES = 10

# El régimen que va PRIMERO en el log reconstruido. Los bloques pares son
# el "base" de Maaradji. Poner a False para invertir la dirección del cambio.
REGIMEN_1_BLOQUES_PARES = True

# Marca temporal de inicio y separación uniforme entre eventos del log reconstruido. Para
# control-flow solo importa el ORDEN de los eventos, no su duración real, así que se
# sintetizan timestamps monótonos equiespaciados.
# Se usa zona horaria UTC (tz-aware)
INICIO = pd.Timestamp("2026-01-01T00:00:00", tz="UTC")
SEPARACION_EVENTOS = pd.Timedelta(minutes=1)


def _cargar_log_con_indice_fisico(ruta_xes: Path) -> pd.DataFrame:
    """
    Carga un XES preservando el orden físico de aparición de cada traza.

    pm4py, al convertir un EventLog a DataFrame, fusiona por `case:concept:name`, lo que
    en estos logs (con ids reutilizados) perdería trazas. Asignamos `trace_real_index` por
    el orden de iteración del EventLog ANTES de convertir, igual que hace el pipeline en
    main_flow, para tener un identificador físico único por traza.
    """
    event_log = pm4py.read_xes(str(ruta_xes), return_legacy_log_object=True)
    for i, trace in enumerate(event_log):
        for event in trace:
            event["trace_real_index"] = i
    return pm4py.convert_to_dataframe(event_log)


def reconstruir(code: str, size: int) -> Path:
    """
    Reconstruye el log `<code>-<size>.xes` como un único cambio al 50% y lo guarda en
    data/01_raw/control_flow/single/<code>-<size>-single.csv.
    """
    ruta_xes = ORIGEN / f"{code}-{size}.xes"
    if not ruta_xes.exists():
        raise FileNotFoundError(f"No existe el log de origen: {ruta_xes}")

    print(f"\n--- Reconstruyendo {code}-{size} ---")
    df = _cargar_log_con_indice_fisico(ruta_xes)

    # Número de trazas físicas y tamaño de bloque (10% del total).
    indices_traza = sorted(df["trace_real_index"].unique())
    n_trazas = len(indices_traza)
    tam_bloque = n_trazas // NUM_BLOQUES
    print(f"Trazas físicas: {n_trazas} | tamaño de bloque: {tam_bloque} (un cambio cada {tam_bloque} trazas)")

    if tam_bloque == 0:
        raise ValueError(f"El log {code}-{size} tiene menos de {NUM_BLOQUES} trazas; no se puede dividir en bloques.")

    # Bloque de cada traza (el remanente, si lo hubiera, se absorbe en el último bloque).
    def bloque_de(t: int) -> int:
        return min(indices_traza.index(t) // tam_bloque, NUM_BLOQUES - 1)

    # Partición por paridad de bloque. Conservamos el orden original dentro de cada régimen.
    pares = [t for t in indices_traza if bloque_de(t) % 2 == 0]
    impares = [t for t in indices_traza if bloque_de(t) % 2 == 1]
    if REGIMEN_1_BLOQUES_PARES:
        nuevo_orden = pares + impares
    else:
        nuevo_orden = impares + pares

    punto_cambio = len(pares) if REGIMEN_1_BLOQUES_PARES else len(impares)
    print(f"Régimen 1: {punto_cambio} trazas | Régimen 2: {n_trazas - punto_cambio} trazas | cambio en la traza {punto_cambio}")

    # Reordenar los eventos juntando las trazas en el nuevo orden y renumerando los case ids
    # de forma secuencial y única (case[0], case[1], ...).
    subframes = []
    case_ids = []
    for nueva_posicion, t in enumerate(nuevo_orden):
        sub = df[df["trace_real_index"] == t].sort_values("time:timestamp")
        subframes.append(sub)
        case_ids.extend([f"case[{nueva_posicion}]"] * len(sub))

    df_nuevo = pd.concat(subframes, ignore_index=True)
    df_nuevo["case:concept:name"] = case_ids

    # Timestamps monótonos equiespaciados en el nuevo orden de eventos.
    df_nuevo["time:timestamp"] = INICIO + np.arange(len(df_nuevo)) * SEPARACION_EVENTOS
    if "start_timestamp" in df_nuevo.columns:
        df_nuevo["start_timestamp"] = df_nuevo["time:timestamp"]

    # El pipeline recalcula trace_real_index al cargar el CSV, así que lo eliminamos.
    df_nuevo = df_nuevo.drop(columns=["trace_real_index"])

    salida = DESTINO / f"{code}-{size}-single.csv"
    df_nuevo.to_csv(salida, index=False)
    print(f"Guardado: {salida} ({len(df_nuevo)} eventos)")
    return salida


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconstruye logs de control-flow de Maaradji con un único cambio al 50%.")
    parser.add_argument("-c", "--codes", nargs="+", default=["cb", "pl"], help="Códigos de patrón de Maaradji (por defecto: cb pl).")
    parser.add_argument("-s", "--sizes", nargs="+", type=int, default=[5000], help="Tamaños de log a reconstruir (por defecto: 5000).")
    args = parser.parse_args()

    for code in args.codes:
        for size in args.sizes:
            reconstruir(code, size)

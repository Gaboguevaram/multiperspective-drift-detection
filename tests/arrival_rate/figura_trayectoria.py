"""
Genera la figura de trayectoria del MAE para el caso AR-A (arrival rate) de la validación por
logs simples.

Molde: tests/control_flow/figura_trayectoria.py (NO el de calendar, que es por-recurso). Como la
métrica de arrival rate es escalar (un único MAE por ventana), la figura tiene UNA sola línea.

Criterio (igual que el resto de perspectivas): se descubre el modelo de referencia UNA vez sobre
el régimen base (primeras `VENTANA_TRAZAS` trazas) y se desliza una ventana de ese mismo tamaño
por todo el log calculando el MAE contra ese modelo FIJO (sin refit). Así se ve cómo, al llegar el
cambio al 50%, el modelo base deja de predecir bien y el MAE sube en escalón.

El preprocesado propio de arrival_rate (codificación cíclica + lags) se replica automáticamente al
reutilizar las funciones reales del pipeline: `modelo_arrival_rate` (que llama a
`preprocesado_arrival_rate` con test=False para el modelo de referencia) y
`calcular_metrica_modelo_sklearn` (que llama a `preprocesado_arrival_rate` con test=True para
evaluar cada ventana).

Guarda un PDF en memoria/Documento/figuras/ y un CSV con la trayectoria en resultados/.
"""

from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.perspectivas.arrival_rate import (
    filtrado_arrival_rate, transformacion_arrival_rate, modelo_arrival_rate,
)
from src.metricas import calcular_metrica_modelo_sklearn

RAIZ = Path(__file__).resolve().parents[2]
DIR_FIGURAS = RAIZ / "memoria" / "Documento" / "figuras"
DIR_RESULTADOS = RAIZ / "resultados"
DIR_RESULTADOS.mkdir(parents=True, exist_ok=True)

ETIQUETA = "AR-A: degradación del modelo de arrival rate (×2 llegadas)"
RUTA_LOG = "data/01_raw/arrival_rate/Caso_A/N-1800-1500-900-750_sudden_5000.csv"
# Parámetros (frecuencia/granularidad/tarea) replicados de conf/logs_simples/ar_a.yml.
PARAMS = {"primera_tarea": "A", "frecuencia_arrival_rate": "30 min",
          "granularidad_arrival_rate": "1 day", "metrica_validacion_modelo": "MAE", "debug": False}
VENTANA_TRAZAS = 400   # tamaño de la ventana deslizante (en trazas)
PASO = 40              # desplazamiento entre ventanas (en trazas)


def trayectoria():
    ruta_log = RAIZ / RUTA_LOG
    print(f"\n=== AR-A: {ruta_log} (ventana={VENTANA_TRAZAS} trazas, paso={PASO}) ===")
    df = pd.read_csv(ruta_log)

    # Índice físico de traza y orden temporal (igual que el molde de control-flow).
    df.sort_values(by="case:concept:name",
                   key=lambda x: x.str.extract(r"(\d+)", expand=False).astype(int), inplace=True)
    df["trace_real_index"] = (df["case:concept:name"] != df["case:concept:name"].shift()).cumsum() - 1
    df.sort_values(by="time:timestamp", inplace=True)
    df["time:timestamp"] = pd.to_datetime(df["time:timestamp"], format="mixed")

    trazas = sorted(df["trace_real_index"].unique())
    n = len(trazas)

    def serie_ventana(ids):
        """Filtra los eventos de las trazas `ids`, fija inicio/fin y devuelve la serie transformada."""
        sub = df[df["trace_real_index"].isin(ids)]
        filtrado = filtrado_arrival_rate(sub, PARAMS)
        p = dict(PARAMS)
        p["inicio"] = filtrado["time:timestamp"].min()
        p["fin"] = filtrado["time:timestamp"].max()
        return transformacion_arrival_rate(filtrado, p), p

    # Modelo de referencia: primeras `VENTANA_TRAZAS` trazas (régimen base, pre-cambio).
    serie_base, p_base = serie_ventana(set(trazas[0:VENTANA_TRAZAS]))
    modelo = modelo_arrival_rate(serie_base, p_base)

    xs, maes = [], []
    for ini in range(0, n - VENTANA_TRAZAS + 1, PASO):
        ids = set(trazas[ini:ini + VENTANA_TRAZAS])
        serie, p = serie_ventana(ids)
        mae = calcular_metrica_modelo_sklearn(serie, modelo, p, "MAE")
        xs.append(max(ids))  # traza más nueva de la ventana
        maes.append(mae)

    # CSV con la trayectoria.
    csv_path = DIR_RESULTADOS / "trayectoria_ar_a.csv"
    pd.DataFrame({"traza": xs, "MAE": maes}).to_csv(csv_path, index=False)

    # Figura: una sola línea (métrica escalar).
    cambio = n // 2
    plt.figure(figsize=(9, 4))
    plt.plot(xs, maes, label="MAE (modelo base fijo)", linewidth=1.8, color="tab:blue")
    plt.axvline(cambio, color="red", linestyle="--", label="cambio real (50%)")
    plt.xlabel("traza")
    plt.ylabel("MAE")
    plt.title(ETIQUETA)
    plt.legend(loc="upper left")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    pdf_path = DIR_FIGURAS / "trayectoria_ar_a.pdf"
    plt.savefig(pdf_path)
    plt.close()
    print(f"Guardado: {pdf_path}\nGuardado: {csv_path}")


if __name__ == "__main__":
    trayectoria()

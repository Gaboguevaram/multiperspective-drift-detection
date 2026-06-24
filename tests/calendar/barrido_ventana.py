"""
Barrido exploratorio del tamaño de ventana para las figuras de trayectoria de calendar.

Para cada caso (cal_a, cal_b) y cada métrica (soporte, soporte invertido) dibuja la curva
por recurso con varios tamaños de ventana en una rejilla, de modo que se pueda elegir a ojo
el tamaño que mejor muestra cada gráfica (la caída clara, pero sin exceso de picos). NO es la
figura final: solo sirve para seleccionar el tamaño por panel.

Genera 4 PDF en resultados/barrido_calendar/.
"""

import argparse
from math import ceil
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.perspectivas.calendar import modelo_calendarios, transformacion_calendarios
from src.metricas import calcular_support, calcular_inverted_support

RAIZ = Path(__file__).resolve().parents[2]
DIR_SALIDA = RAIZ / "resultados" / "barrido_calendar"
DIR_SALIDA.mkdir(parents=True, exist_ok=True)

CASOS = {
    "cal_a": ("CAL-A (reducción)", "data/01_raw/calendar/Caso_D/Rc_sudden_caso_d_5000.csv"),
    "cal_b": ("CAL-B (ampliación)", "data/01_raw/calendar/Caso_C/Ac_sudden_caso_c_5000.csv"),
}
VENTANAS = [150, 200, 250, 300, 350, 400, 500]
PASO = 25


def _corto(r: str) -> str:
    return r.split("_", 1)[1] if "_" in r else r


def cargar(ruta: Path) -> pd.DataFrame:
    df = pd.read_csv(ruta)
    df["time:timestamp"] = pd.to_datetime(df["time:timestamp"], format="mixed")
    df.sort_values(by="case:concept:name",
                   key=lambda x: x.str.extract(r"(\d+)", expand=False).astype(int), inplace=True)
    df["trace_real_index"] = (df["case:concept:name"] != df["case:concept:name"].shift()).cumsum() - 1
    df.sort_values(by="time:timestamp", inplace=True)
    return transformacion_calendarios(df, {"debug": False})


def trayectoria(df: pd.DataFrame, ventana: int):
    config = {"debug": False}
    trazas = sorted(df["trace_real_index"].unique())
    n = len(trazas)
    ref_ids = set(trazas[0:ventana])
    modelo = modelo_calendarios(df[df["trace_real_index"].isin(ref_ids)], config)
    recursos = modelo["recursos"]
    xs = []
    sup = {r: [] for r in recursos}
    sinv = {r: [] for r in recursos}
    for ini in range(0, n - ventana + 1, PASO):
        ids = set(trazas[ini:ini + ventana])
        v = df[df["trace_real_index"].isin(ids)]
        xs.append(max(ids))
        s = calcular_support(v, modelo, config, "soporte")
        si = calcular_inverted_support(v, modelo, config, "soporte_invertido")
        for r in recursos:
            sup[r].append(s.get(r, float("nan")))
            sinv[r].append(si.get(r, float("nan")))
    return xs, sup, sinv, recursos, n


def barrido(clave: str):
    etiqueta, ruta_rel = CASOS[clave]
    print(f"\n=== {clave}: {ruta_rel} ===")
    df = cargar(RAIZ / ruta_rel)
    # Se calcula una vez por ventana (ambas métricas salen del mismo recorrido).
    resultados = {}
    for v in VENTANAS:
        print(f"  ventana={v}...")
        resultados[v] = trayectoria(df, v)

    for metrica in ("soporte", "soporte_inv"):
        ncols = 3
        nrows = ceil(len(VENTANAS) / ncols)
        fig, axes = plt.subplots(nrows, ncols, figsize=(12, 3.0 * nrows), squeeze=False)
        for i, v in enumerate(VENTANAS):
            xs, sup, sinv, recursos, n = resultados[v]
            datos = sup if metrica == "soporte" else sinv
            ax = axes[i // ncols][i % ncols]
            for r in recursos:
                ax.plot(xs, datos[r], linewidth=1.5, label=_corto(r))
            ax.axvline(n // 2, color="red", linestyle="--", linewidth=1, label="cambio (50%)")
            ax.set_title(f"ventana = {v}")
            ax.set_xlabel("traza")
            ax.grid(alpha=0.3)
            ax.legend(loc="best", fontsize=7)
        for j in range(len(VENTANAS), nrows * ncols):
            axes[j // ncols][j % ncols].axis("off")
        fig.suptitle(f"{etiqueta}: {metrica} — barrido de tamaño de ventana")
        fig.tight_layout()
        out = DIR_SALIDA / f"barrido_{clave}_{metrica}.pdf"
        fig.savefig(out)
        plt.close(fig)
        print(f"Guardado: {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Barrido de tamaño de ventana para las figuras de calendar")
    parser.add_argument("-c", "--casos", nargs="+", default=["cal_a", "cal_b"], help="Casos (cal_a, cal_b)")
    args = parser.parse_args()
    for clave in args.casos:
        barrido(clave)

"""
Genera la figura de trayectoria del soporte y el soporte invertido POR RECURSO para los
casos de calendar de la validación por logs simples (CAL-A = Caso D reducción,
CAL-B = Caso C ampliación).

Misma lógica que tests/control_flow/figura_trayectoria.py: descubre un modelo de
calendarios sobre el régimen base (primeras `ventana_trazas` trazas) y desliza una ventana
de ese mismo tamaño por todo el log calculando las métricas contra ese modelo fijo. La
diferencia es que en calendar las métricas son POR RECURSO ({recurso: valor}), así que se
dibuja una línea por recurso: al llegar el cambio al 50% solo se mueve la métrica y el
recurso afectados, mientras el recurso de control queda plano.

Guarda un PDF en memoria/Documento/figuras/ y un CSV con la trayectoria en resultados/.
"""

import argparse
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.perspectivas.calendar import modelo_calendarios, transformacion_calendarios
from src.metricas import calcular_support, calcular_inverted_support

RAIZ = Path(__file__).resolve().parents[2]
DIR_FIGURAS = RAIZ / "memoria" / "Documento" / "figuras"
DIR_RESULTADOS = RAIZ / "resultados"
DIR_RESULTADOS.mkdir(parents=True, exist_ok=True)

# Casos: (etiqueta, ruta_log, ventana en trazas, paso en trazas)
CASOS = {
    "cal_a": ("CAL-A (reducción): cae el soporte del recurso que cambia",
              "data/01_raw/calendar/Caso_D/Rc_sudden_caso_d_5000.csv", 300, 25),
    "cal_b": ("CAL-B (ampliación): sube el soporte invertido del recurso que cambia",
              "data/01_raw/calendar/Caso_C/Ac_sudden_caso_c_5000.csv", 300, 25),
}


def _nombre_corto(recurso: str) -> str:
    # 'resource_Marta' -> 'Marta'
    return recurso.split("_", 1)[1] if "_" in recurso else recurso


def trayectoria(clave: str):
    etiqueta, ruta_rel, ventana_trazas, paso = CASOS[clave]
    ruta_log = RAIZ / ruta_rel

    print(f"\n=== {clave}: {ruta_log} (ventana={ventana_trazas} trazas, paso={paso}) ===")
    df = pd.read_csv(ruta_log)

    # Índice físico de traza y orden temporal.
    df["time:timestamp"] = pd.to_datetime(df["time:timestamp"], format="mixed")
    df.sort_values(by="case:concept:name",
                   key=lambda x: x.str.extract(r"(\d+)", expand=False).astype(int), inplace=True)
    df["trace_real_index"] = (df["case:concept:name"] != df["case:concept:name"].shift()).cumsum() - 1
    df.sort_values(by="time:timestamp", inplace=True)

    config = {"debug": False}

    # El descubrimiento de calendarios exige el formato de pix-framework: se aplica la MISMA
    # transformación que usa la perspectiva (timestamps a UTC con ISO8601, recurso a str, orden),
    # en lugar de convertir las columnas a mano. Conserva trace_real_index (no la renombra/filtra).
    df = transformacion_calendarios(df, config)

    trazas = sorted(df["trace_real_index"].unique())
    n = len(trazas)

    # Modelo de referencia: primeras `ventana_trazas` trazas (régimen base, pre-cambio).
    ref_ids = set(trazas[0:ventana_trazas])
    modelo = modelo_calendarios(df[df["trace_real_index"].isin(ref_ids)], config)
    recursos = modelo["recursos"]

    xs = []
    sup = {r: [] for r in recursos}
    sinv = {r: [] for r in recursos}
    for ini in range(0, n - ventana_trazas + 1, paso):
        ids = set(trazas[ini:ini + ventana_trazas])
        v = df[df["trace_real_index"].isin(ids)]
        xs.append(max(ids))  # traza más nueva de la ventana
        s = calcular_support(v, modelo, config, "soporte")
        si = calcular_inverted_support(v, modelo, config, "soporte_invertido")
        for r in recursos:
            # Recurso ausente en la ventana -> NaN (hueco en la curva, no se inventa valor).
            sup[r].append(s.get(r, float("nan")))
            sinv[r].append(si.get(r, float("nan")))

    # CSV con la trayectoria.
    datos = {"traza": xs}
    for r in recursos:
        datos[f"soporte_{_nombre_corto(r)}"] = sup[r]
        datos[f"soporte_inv_{_nombre_corto(r)}"] = sinv[r]
    csv_path = DIR_RESULTADOS / f"trayectoria_{clave}.csv"
    pd.DataFrame(datos).to_csv(csv_path, index=False)

    # Figura: dos paneles (soporte y soporte invertido), una línea por recurso.
    cambio = n // 2
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 7), sharex=True)
    for r in recursos:
        etiqueta_r = _nombre_corto(r)
        ax1.plot(xs, sup[r], linewidth=1.8, label=etiqueta_r)
        ax2.plot(xs, sinv[r], linewidth=1.8, label=etiqueta_r)

    for ax, titulo in ((ax1, "soporte"), (ax2, "soporte invertido")):
        ax.axvline(cambio, color="red", linestyle="--", label="cambio real (50%)")
        ax.set_ylabel(titulo)
        ax.set_ylim(-0.05, 1.05)
        ax.grid(alpha=0.3)
        ax.legend(loc="best")
    ax2.set_xlabel("traza")
    fig.suptitle(etiqueta)
    fig.tight_layout()
    pdf_path = DIR_FIGURAS / f"trayectoria_{clave}.pdf"
    fig.savefig(pdf_path)
    plt.close(fig)
    print(f"Guardado: {pdf_path}\nGuardado: {csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Figura de trayectoria soporte/soporte invertido por recurso para calendar")
    parser.add_argument("-c", "--casos", nargs="+", default=["cal_a", "cal_b"], help="Casos a generar (cal_a, cal_b)")
    args = parser.parse_args()
    for clave in args.casos:
        trayectoria(clave)

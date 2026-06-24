import os
import re
import sys
import optuna
import subprocess

# ---------------------------------------------------------------------------
# Búsqueda exhaustiva del MEJOR TAMAÑO de ventana de TRAZAS para control-flow.
#
# Recorre TODOS los candidatos de tamaño una sola
# vez (búsqueda exhaustiva real) en lugar de muestrear al azar con TPE.
# ---------------------------------------------------------------------------

# Logs sobre los que buscar la mejor ventana. Comenta los que no quieras lanzar.
LOGS = [
    "./data/01_raw/control_flow/recurring/cm-2500.xes",
    "./data/01_raw/control_flow/recurring/cm-5000.xes",
    "./data/01_raw/control_flow/recurring/cm-7500.xes",
    "./data/01_raw/control_flow/recurring/cm-10000.xes",
    "./data/01_raw/control_flow/recurring/cp-2500.xes",
    "./data/01_raw/control_flow/recurring/cp-5000.xes",
    "./data/01_raw/control_flow/recurring/cp-7500.xes",
    "./data/01_raw/control_flow/recurring/cp-10000.xes",
    "./data/01_raw/control_flow/recurring/pm-2500.xes",
    "./data/01_raw/control_flow/recurring/pm-5000.xes",
    "./data/01_raw/control_flow/recurring/pm-7500.xes",
    "./data/01_raw/control_flow/recurring/pm-10000.xes",
    "./data/01_raw/control_flow/recurring/re-2500.xes",
    "./data/01_raw/control_flow/recurring/re-5000.xes",
    "./data/01_raw/control_flow/recurring/re-7500.xes",
    "./data/01_raw/control_flow/recurring/re-10000.xes",
    "./data/01_raw/control_flow/recurring/rp-2500.xes",
    "./data/01_raw/control_flow/recurring/rp-5000.xes",
    "./data/01_raw/control_flow/recurring/rp-7500.xes",
    "./data/01_raw/control_flow/recurring/rp-10000.xes",
    "./data/01_raw/control_flow/recurring/sw-2500.xes",
    "./data/01_raw/control_flow/recurring/sw-5000.xes",
    "./data/01_raw/control_flow/recurring/sw-7500.xes",
    "./data/01_raw/control_flow/recurring/sw-10000.xes",
    "./data/01_raw/control_flow/recurring/cb-2500.xes",
    "./data/01_raw/control_flow/recurring/cb-5000.xes",
    "./data/01_raw/control_flow/recurring/cb-7500.xes",
    "./data/01_raw/control_flow/recurring/cb-10000.xes",
    "./data/01_raw/control_flow/recurring/lp-2500.xes",
    "./data/01_raw/control_flow/recurring/lp-5000.xes",
    "./data/01_raw/control_flow/recurring/lp-7500.xes",
    "./data/01_raw/control_flow/recurring/lp-10000.xes",
    "./data/01_raw/control_flow/recurring/cd-2500.xes",
    "./data/01_raw/control_flow/recurring/cd-5000.xes",
    "./data/01_raw/control_flow/recurring/cd-7500.xes",
    "./data/01_raw/control_flow/recurring/cd-10000.xes",
    "./data/01_raw/control_flow/recurring/cf-2500.xes",
    "./data/01_raw/control_flow/recurring/cf-5000.xes",
    "./data/01_raw/control_flow/recurring/cf-7500.xes",
    "./data/01_raw/control_flow/recurring/cf-10000.xes",
    "./data/01_raw/control_flow/recurring/pl-2500.xes",
    "./data/01_raw/control_flow/recurring/pl-5000.xes",
    "./data/01_raw/control_flow/recurring/pl-7500.xes",
    "./data/01_raw/control_flow/recurring/pl-10000.xes",
    "./data/01_raw/control_flow/recurring/ior-2500.xes",
    "./data/01_raw/control_flow/recurring/ior-5000.xes",
    "./data/01_raw/control_flow/recurring/ior-7500.xes",
    "./data/01_raw/control_flow/recurring/ior-10000.xes",
    "./data/01_raw/control_flow/recurring/iro-2500.xes",
    "./data/01_raw/control_flow/recurring/iro-5000.xes",
    "./data/01_raw/control_flow/recurring/iro-7500.xes",
    "./data/01_raw/control_flow/recurring/iro-10000.xes",
    "./data/01_raw/control_flow/recurring/oir-2500.xes",
    "./data/01_raw/control_flow/recurring/oir-5000.xes",
    "./data/01_raw/control_flow/recurring/oir-7500.xes",
    "./data/01_raw/control_flow/recurring/oir-10000.xes",
    "./data/01_raw/control_flow/recurring/ori-2500.xes",
    "./data/01_raw/control_flow/recurring/ori-5000.xes",
    "./data/01_raw/control_flow/recurring/ori-7500.xes",
    "./data/01_raw/control_flow/recurring/ori-10000.xes",
    "./data/01_raw/control_flow/recurring/roi-2500.xes",
    "./data/01_raw/control_flow/recurring/roi-5000.xes",
    "./data/01_raw/control_flow/recurring/roi-7500.xes",
    "./data/01_raw/control_flow/recurring/roi-10000.xes",
    "./data/01_raw/control_flow/recurring/rio-2500.xes",
    "./data/01_raw/control_flow/recurring/rio-5000.xes",
    "./data/01_raw/control_flow/recurring/rio-7500.xes",
    "./data/01_raw/control_flow/recurring/rio-10000.xes",
]

# Rejilla de tamaños de ventana (en trazas), FIJA e igual para todos los logs,
# tal como hace la tesis: el autoajuste de Víctor busca el tamaño en el rango
# 0-250, así que probamos esos mismos valores de forma exhaustiva.
TAMANOS_CANDIDATOS = [25, 50, 75, 100, 125, 150, 175, 200, 225, 250]

# Fichero donde se va volcando, log a log, la mejor ventana encontrada.
ARCHIVO_RESULTADOS = "./resultados/ventana_trazas_optima_control_flow.csv"


def hacer_objective(ruta_log):
    """Crea la función objetivo de Optuna cerrada sobre el log actual."""

    def objective(trial):

        # --- A. ÚNICO PARÁMETRO LIBRE: el tamaño de la ventana de trazas ---
        tamano_ventana = trial.suggest_categorical('tamano_ventana', TAMANOS_CANDIDATOS)

        # Reglas impuestas: salto=1 (en la plantilla), n_conf=tamano, n_reg=tamano//2.
        n_confirmacion = tamano_ventana
        n_regresion = n_confirmacion // 2

        # --- B. LEER LA PLANTILLA Y REEMPLAZAR ---
        with open("./tests/control_flow/params_template_trazas.yml", "r", encoding="utf-8") as f:
            contenido = f.read()

        contenido = contenido.replace("__TAMANO_VENTANA__", str(tamano_ventana))
        contenido = contenido.replace("__N_CONFIRMACION__", str(n_confirmacion))
        contenido = contenido.replace("__N_REGRESION__", str(n_regresion))

        # --- C. ESCRIBIR EL CONFIG REAL ---
        archivo_config = f"./conf/pruebas/control_flow_trazas_{trial.number}.yml"
        with open(archivo_config, "w", encoding="utf-8") as f:
            f.write(contenido)

        # --- D. EJECUTAR EL BENCHMARK (un solo log objetivo) ---
        comando = [sys.executable, "-m", "tests.control_flow.script_control_flow",
                   "--file", archivo_config, "--log", ruta_log]

        try:
            resultado_proceso = subprocess.run(
                comando, cwd=".", capture_output=True, text=True, check=True
            )
            salida_texto = resultado_proceso.stdout

        except subprocess.CalledProcessError as e:
            print(f"[ERROR] El programa falló con error: {e}")
            print(f"[ERROR] stdout:\n{e.stdout}\n")
            print(f"[ERROR] stderr:\n{e.stderr}\n")
            return -9999999.0
        except Exception as e:
            print(f"[ERROR] Error inesperado al ejecutar el subproceso: {e}")
            return -9999999.0

        # --- E. LEER EL RESULTADO ---
        match = re.search(r"Media total de F-Scores: ([\d.]+)", salida_texto)
        if not match:
            print("Error: No se encontró el resultado en la salida del programa.")
            print(f"DEBUG: Salida completa:\n{salida_texto}\n")
            return -9999999.0

        try:
            f_score = float(match.group(1).strip())
        except ValueError as e:
            print(f"Error al convertir el F-Score a float: {e}")
            return -9999999.0

        print(f"[{os.path.basename(ruta_log)}] tamano={tamano_ventana} "
              f"(n_conf={n_confirmacion}, n_reg={n_regresion}) -> F-Score={f_score:.4f}")
        return f_score

    return objective


def mejor_trial(study):
    """Mejor F-Score y, en caso de empate, la ventana MÁS PEQUEÑA (menos retardo)."""
    completados = [t for t in study.trials if t.value is not None]
    return max(completados, key=lambda t: (t.value, -t.params['tamano_ventana']))


def guardar_resultado(ruta_log, mejor_tamano, mejor_f_score):
    """Añade una fila al CSV de resultados (lo crea con cabecera si no existe)."""
    os.makedirs(os.path.dirname(ARCHIVO_RESULTADOS), exist_ok=True)
    nuevo = not os.path.exists(ARCHIVO_RESULTADOS)
    with open(ARCHIVO_RESULTADOS, "a", encoding="utf-8") as f:
        if nuevo:
            f.write("log,tamano_ventana,n_confirmacion,n_regresion,f_score\n")
        f.write(f"{ruta_log},{mejor_tamano},{mejor_tamano},{mejor_tamano // 2},{mejor_f_score:.4f}\n")


if __name__ == "__main__":

    # Permite lanzar un solo log: python -m tests.control_flow.optimizacion_ventana_trazas <ruta_log>
    logs = [sys.argv[1]] if len(sys.argv) > 1 else LOGS

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    resumen = []

    for ruta_log in logs:
        print("\n" + "=" * 60)
        print(f"Optimizando {ruta_log}")
        print(f"Candidatos de tamaño (trazas): {TAMANOS_CANDIDATOS}")
        print("=" * 60)

        # GridSampler -> búsqueda exhaustiva: un trial por candidato, sin repetir.
        sampler = optuna.samplers.GridSampler({'tamano_ventana': TAMANOS_CANDIDATOS})
        study = optuna.create_study(direction="maximize", sampler=sampler)
        study.optimize(hacer_objective(ruta_log), n_trials=len(TAMANOS_CANDIDATOS))

        mejor = mejor_trial(study)
        mejor_tamano, mejor_f_score = mejor.params['tamano_ventana'], mejor.value
        guardar_resultado(ruta_log, mejor_tamano, mejor_f_score)
        resumen.append((ruta_log, mejor_tamano, mejor_f_score))

        print(f"--> MEJOR para {os.path.basename(ruta_log)}: "
              f"tamano={mejor_tamano} | F-Score={mejor_f_score:.4f}")

    print("\n" + "#" * 60)
    print("RESUMEN (mejor ventana por log):")
    for ruta_log, tamano, f_score in resumen:
        print(f"  {os.path.basename(ruta_log):<20} tamano={tamano:<5} F-Score={f_score:.4f}")
    print(f"\nResultados guardados en {ARCHIVO_RESULTADOS}")
    print("#" * 60)

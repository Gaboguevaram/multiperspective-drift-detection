import re
import optuna
import subprocess
import sys

# Número de pruebas de Optuna. Subir para una búsqueda más fina (más lento).
N_TRIALS = 150

# Log sobre el que se optimiza. Se optimiza UN log a la vez (un único cambio sostenido al 50%).
# CAL-A (reducción de jornada -> cae el soporte). Para CAL-B (ampliación -> sube el soporte
# inverso) 
# 
# Logs:
# "./data/01_raw/calendar/CAL-A/cal_a-5000-single.csv"  (reducción de jornada)
# "./data/01_raw/calendar/CAL-B/cal_b-5000-single.csv"  (ampliación de jornada)
LOG_OBJETIVO = "./data/01_raw/calendar/CAL-A/cal_a-5000-single.csv"


def convertir_a_minutos(texto):
    conversion = {
        '13 min': 13,
        '30 min': 30,
        '1 hour': 60,
        '2 hours': 120,
        '4 hours': 240,
        '6 hours': 360,
        '9 hours': 540,
        '12 hours': 720,
        '18 hours': 1080,
        '1 day': 1440,
        '2 days': 2880,
        '3 days': 4320,
        '4 days': 5760,
        '5 days': 7200,
        '7 days': 10080,
        '10 days': 14400,
        '14 days': 20160,
        '21 days': 30240,
        '30 days': 43200,
    }
    return conversion.get(texto, None)

def objective(trial):

    # --- A. ESPACIO DE BÚSQUEDA ---
    # El soporte de calendario se mide sobre jornadas, así que la escala es de días.
    tamano_ventana = trial.suggest_categorical('tamano_ventana', [
        '6 hours', '9 hours', '12 hours',
        '1 day', '3 days', '5 days', '7 days', '10 days',
        '14 days'
    ])

    salto_ventana = trial.suggest_categorical('salto_ventana', [
        '30 min', '1 hour', '2 hours', '4 hours', '6 hours', '9 hours', '12 hours', '18 hours',
        '1 day', '2 days', '3 days', '5 days', '7 days'
    ])

    n_confirmacion = trial.suggest_int('n_confirmacion', 50, 300, step=10)

    # --- VALIDACIÓN DE REGLAS LÓGICAS ---
    tamano_val = convertir_a_minutos(tamano_ventana)
    salto_val = convertir_a_minutos(salto_ventana)

    # Regla 1: tamano_ventana > salto_ventana
    if not (tamano_val > salto_val):
        raise optuna.TrialPruned()

    # Regla 2: n_regresion = n_confirmacion / 2
    n_regresion = n_confirmacion // 2

    # --- B. LEER LA PLANTILLA Y REEMPLAZAR ---
    with open("./tests/calendar/params_template.yml", "r", encoding="utf-8") as f:
        contenido = f.read()

    contenido = contenido.replace("__TAMANO_VENTANA__", str(tamano_ventana))
    contenido = contenido.replace("__SALTO_VENTANA__", str(salto_ventana))
    contenido = contenido.replace("__N_CONFIRMACION__", str(n_confirmacion))
    contenido = contenido.replace("__N_REGRESION__", str(n_regresion))

    # --- C. ESCRIBIR EL CONFIG REAL ---
    archivo_config = f"./conf/pruebas/calendar_tests_{trial.number}.yml"
    with open(archivo_config, "w", encoding="utf-8") as f:
        f.write(contenido)

    print(f"DEBUG: Contenido generado en {archivo_config}:\n{contenido}\n")

    # --- D. EJECUTAR EL BENCHMARK (un solo log objetivo) ---
    comando = [sys.executable, "-m", "tests.calendar.script_calendar",
               "--file", archivo_config, "--log", LOG_OBJETIVO]

    try:
        resultado_proceso = subprocess.run(
            comando,
            cwd=".",
            capture_output=True,
            text=True,
            check=True
        )
        salida_texto = resultado_proceso.stdout
        print(f"DEBUG: Salida estándar del programa:\n{salida_texto}\n")

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] El programa falló con error: {e}")
        print(f"[ERROR] Salida estándar del error:\n{e.stdout}\n")
        print(f"[ERROR] Salida de error estándar:\n{e.stderr}\n")
        return -9999999.0
    except Exception as e:
        print(f"DEBUG: Error inesperado al ejecutar el subproceso: {e}")
        return -9999999.0

    # --- E. LEER EL RESULTADO ---
    match = re.search(r"Media total de F-Scores: ([\d.]+)", salida_texto)

    if match:
        valor_capturado = match.group(1).strip()
        if not valor_capturado:
            print("Error: Se encontró 'Media total de F-Scores:' pero el valor está vacío.")
            return -9999999.0
        try:
            f_score = float(valor_capturado)
            print(f"Trial finalizado. F-Score={f_score:.4f} | tamano_ventana={tamano_ventana}, "
                  f"salto_ventana={salto_ventana}, n_regresion={n_regresion}, n_confirmacion={n_confirmacion}")
            return f_score
        except ValueError as e:
            print(f"Error al convertir '{valor_capturado}' a float: {e}")
            return -9999999.0
    else:
        print("Error: No se encontró el resultado en la salida del programa.")
        print(f"DEBUG: Salida completa del programa:\n{salida_texto}\n")
        return -9999999.0


def parar_si_perfecto(study, trial):
    """Detiene la optimización en cuanto un trial alcanza F-score 1.0: ya no hay nada mejor."""
    if trial.value is not None and trial.value >= 1.0:
        print("F-Score perfecto (1.0) alcanzado. Deteniendo la optimización.")
        study.stop()


# --- F. LANZAR OPTUNA ---
if __name__ == "__main__":
    study = optuna.create_study(direction="maximize")

    print("Iniciando optimización...")
    study.optimize(objective, n_trials=N_TRIALS, show_progress_bar=True, callbacks=[parar_si_perfecto])

    print("\n----------------------------------")
    print("MEJORES PARÁMETROS ENCONTRADOS:")
    print(study.best_params)
    print(f"Mejor F-Score: {study.best_value:.4f}")
    print("----------------------------------")

import argparse
import subprocess
import statistics

# Logs de control-flow con un único cambio sostenido al 50% (reconstruidos de Maaradji).
# cb -> caída de fitness, pl -> caída de precisión. Se puede recortar la lista a un solo
# log para que la optimización con Optuna vaya más rápida.
LOGS = [
    "./data/01_raw/control_flow/single/cb-5000-single.csv",
    "./data/01_raw/control_flow/single/pl-5000-single.csv",
]

def main(config_file, logs=None):
    f_scores = []

    logs = logs if logs else LOGS

    for log in logs:
        print(f"Lanzando: {log}")

        # Construimos el comando
        comando = ["python", "-m", "tests.test_perspectivas", "--log", log, "--perspectiva", "control_flow", "--file", config_file]

        try:
            # run espera a que termine y captura lo que el programa imprime (stdout)
            resultado = subprocess.run(comando, capture_output=True, text=True, check=True)

            # El F-Score es la última línea no vacía que imprime test_perspectivas.
            lineas_output = [linea.strip() for linea in resultado.stdout.split('\n') if linea.strip()]

            if lineas_output:
                f_score_str = lineas_output[-1]
                f_score = float(f_score_str)
                f_scores.append(f_score)
                print(f" => F-Score obtenido: {f_score}")
            else:
                print(f" => No se encontró salida para parsear el F-score en {log}")

        except subprocess.CalledProcessError as e:
            print(f"Error al ejecutar el comando para {log}:\n{e.stderr}")
        except ValueError:
            print(f"No se pudo convertir el F-Score a número. Salida original: '{f_score_str}'")

    print("\nTodos los comandos completados.")

    if f_scores:
        media = statistics.mean(f_scores)
        print(f"---------------------------------------------------")
        print(f"Media total de F-Scores: {media:.4f}")
        print(f"---------------------------------------------------")
    else:
        print("No se calcularon F-Scores (la lista está vacía).")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Lanza el benchmark de control-flow sobre los logs de un único cambio')
    parser.add_argument('-f', '--file', help='Indica el archivo de configuración .yml a usar', type=str, default="./conf/logs_simples/cf_cb.yml")
    parser.add_argument('-l', '--log', help='Si se indica, evalúa solo sobre este log en vez de la lista LOGS.', type=str, default=None)
    args = parser.parse_args()

    main(args.file, [args.log] if args.log else None)

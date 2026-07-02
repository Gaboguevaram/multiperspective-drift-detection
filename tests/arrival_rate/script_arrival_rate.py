import argparse
import subprocess
import statistics

# Lista de logs
LOGS = [
    "./data/01_raw/arrival_rate/AR-A/ar_a-5000-single.csv",
]

def main(config_file, logs=None):
    f_scores = []

    logs = logs if logs else LOGS

    # Nos aseguramos de ejecutar desde la raíz del proyecto
    # current_dir = os.getcwd() # Asume que lo ejecutas desde la raíz, igual que el bash

    for log in logs:
        print(f"Lanzando: {log}")
        
        # Construimos el comando
        comando = ["python", "-m", "tests.test_perspectivas", "--log", log, "--perspectiva", "arrival_rate","--file", config_file]
        
        try:
            # Ejecutamos el comando, capturando la salida (stdout) y los errores (stderr)
            resultado = subprocess.run(comando, capture_output=True, text=True, check=True)
            
            # Asumimos que el F-Score devuelto está en la salida estándar.
            # Puedes ajustar esta lógica dependiendo de cómo imprime el output `tests.test_perspectivas`
            # En este caso, cogemos la última línea no vacía e intentamos convertirla a float.
            lineas_output = [linea.strip() for linea in resultado.stdout.split('\n') if linea.strip()]
            
            if lineas_output:
                # Tomamos la última línea devuelta (ajusta esto si el script devuelve otra cosa junto con el f-score)
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

    # Configuración de los parámetros de entrada
    parser = argparse.ArgumentParser(description='Herramienta de minería de procesos multiperspectiva')
    
    # Archivo de configuración
    parser.add_argument('-f', '--file', help='Indica el archivo de configuración .yml a usar', type=str, default="./conf/arrival_rate.yml")
    parser.add_argument('-l', '--log', help='Si se indica, evalúa solo sobre este log en vez de la lista LOGS.', type=str, default=None)

    args = parser.parse_args()

    main(args.file, [args.log] if args.log else None)
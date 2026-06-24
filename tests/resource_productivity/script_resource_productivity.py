import argparse
import subprocess
import statistics

# Lista de logs
LOGS = [
    "./data/01_raw/resource_productivity/Caso_E/TP_sudden_caso_e_2500.csv",
    "./data/01_raw/resource_productivity/Caso_E/TP_sudden_caso_e_5000.csv",
    "./data/01_raw/resource_productivity/Caso_E/TP_sudden_caso_e_7500.csv",
]

def main(config_file, logs=None):
    f_scores = []

    logs = logs if logs else LOGS

    for log in logs:
        print(f"Lanzando: {log}")
        
        # Construimos el comando
        comando = ["python", "-m", "tests.test_perspectivas", "--log", log, "--perspectiva", "resource_productivity", "--file", config_file]
        
        try:
            # Ejecutamos el comando, capturando la salida (stdout) y los errores (stderr)
            resultado = subprocess.run(comando, capture_output=True, text=True, check=True)
            
            # Asumimos que el F-Score devuelto está en la salida estándar.
            lineas_output = [linea.strip() for linea in resultado.stdout.split('\n') if linea.strip()]
            
            if lineas_output:
                # Tomamos la última línea devuelta
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
    parser.add_argument('-f', '--file', help='Indica el archivo de configuración .yml a usar', type=str, default="./conf/resource_productivity.yml")
    parser.add_argument('-l', '--log', help='Si se indica, evalúa solo sobre este log en vez de la lista LOGS.', type=str, default=None)

    args = parser.parse_args()

    main(args.file, [args.log] if args.log else None)

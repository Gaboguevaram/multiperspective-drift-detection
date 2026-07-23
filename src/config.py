import yaml
from pathlib import Path

# Raíz del proyecto (el directorio que contiene src/, conf/ y data/).
RAIZ_PROYECTO = Path(__file__).resolve().parent.parent

# Capas de la carpeta data/. El pipeline escribe en ellas durante la ejecución
# (logs filtrados, transformados, modelos descubiertos, métricas, informes...).
CAPAS_DATOS = (
    "01_raw",
    "02_intermediate",
    "03_primary",
    "04_feature",
    "05_model_input",
    "06_models",
    "07_model_output",
    "08_reporting",
)

# Directorios de salida fuera de data/.
DIRECTORIOS_SALIDA = ("logs", "resultados")


def asegurar_directorios_salida(raiz: Path = None) -> None:
    """
    Crea las capas de `data/` y los directorios de salida si no existen.

    El pipeline escribe en estas rutas desde muchos puntos distintos sin crearlas
    previamente, así que basta con invocar esta función una vez al arrancar la
    ejecución. Es idempotente: si los directorios ya existen no hace nada.

    Args:
        raiz: Raíz del proyecto. Por defecto, la del propio paquete.
    """
    raiz = Path(raiz) if raiz is not None else RAIZ_PROYECTO

    for capa in CAPAS_DATOS:
        (raiz / "data" / capa).mkdir(parents=True, exist_ok=True)

    for directorio in DIRECTORIOS_SALIDA:
        (raiz / directorio).mkdir(parents=True, exist_ok=True)


def cargar_parametros(ruta: str = None):
    """Lee el archivo YAML y lo devuelve como un diccionario de Python."""
    if ruta is None:
        disponibles = sorted(
            str(p.relative_to(RAIZ_PROYECTO)) for p in (RAIZ_PROYECTO / "conf").rglob("*.yml")
        )
        raise ValueError(
            "No se ha indicado un fichero de configuración. Use -f/--file con la ruta "
            "de un YAML de conf/.\nFicheros disponibles:\n  "
            + "\n  ".join(disponibles)
        )

    ruta = Path(ruta)
    if not ruta.is_file():
        raise FileNotFoundError(f"No existe el fichero de configuración: {ruta}")

    with open(ruta, "r", encoding="utf-8") as archivo:
        return yaml.safe_load(archivo)

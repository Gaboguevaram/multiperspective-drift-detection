import yaml
from pathlib import Path

def cargar_parametros(ruta: str = None):
    """Lee el archivo YAML y lo devuelve como un diccionario de Python."""
    if ruta is None:
        ruta = Path(__file__).parent.parent / "conf" / "parameters.yml"
    with open(ruta, "r") as archivo:
        return yaml.safe_load(archivo)
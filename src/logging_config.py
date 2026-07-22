import logging
import logging.handlers
import pandas as pd

from pathlib import Path


def setup_logging(
    log_dir: str = "./logs",
    log_file: str = f"drift_detection{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.log",
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """
    Configura el sistema de logging para toda la aplicación.

    Crea un logger raíz con dos handlers:
    1. FileHandler: Guarda TODOS los logs (DEBUG y superior) en archivo
    2. StreamHandler (consola): Solo muestra logs INFO y superior en consola

    Args:
        log_dir: Directorio donde se guardarán los archivos de log (default: ./logs)
        log_file: Nombre del archivo de log (default: drift_detection.log)
        console_level: Nivel mínimo para la consola (default: logging.INFO)
        file_level: Nivel mínimo para el archivo (default: logging.DEBUG)

    Returns:
        logging.Logger: El logger raíz configurado
    """

    # Crear el directorio de logs si no existe
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Obtener el logger raíz
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # El logger raíz debe aceptar todos los niveles

    # Evitar duplicados si el logger ya está configurado
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formato consistente para todos los handlers
    # Ejemplo: 2026-03-20 14:30:45,123 - INFO - drift_detection:45 - Mensaje de log
    format_string = (
        "%(asctime)s - %(levelname)-8s - %(name)s:%(lineno)d - %(message)s"
    )
    formatter = logging.Formatter(format_string)

    # --- Handler para archivo ---
    file_path = log_path / log_file
    file_handler = logging.FileHandler(file_path, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # --- Handler para consola ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Silenciar el ruido de las librerías de terceros, que de otro modo oculta por
    # completo la salida del framework:
    #
    # - httpx/httpcore: registran una línea por cada petición al servidor temporal
    #   de Prefect, es decir, una por cada mensaje de log emitido.
    # - prefect.events.utilities y prefect._internal.concurrency: el servicio de
    #   telemetría de eventos falla al arrancar y al apagarse el servidor efímero
    #   ("Service 'EventsWorker' failed...", "Cannot put items in a stopped service
    #   instance"), registrando un traceback completo por evento (cientos en una
    #   ejecución normal). Son inofensivos —esos eventos solo alimentan la interfaz
    #   de Prefect, que en modo efímero se descarta al terminar—, pero dan la falsa
    #   impresión de que la ejecución ha fallado.
    for libreria in (
        "httpx",
        "httpcore",
        "prefect.events.utilities",
        "prefect._internal.concurrency",
    ):
        logging.getLogger(libreria).setLevel(logging.CRITICAL)

    return logger


def get_logger(module_name: str) -> logging.Logger:
    """
    Obtiene un logger para un módulo específico.

    Esta función debe usarse en cada módulo para obtener su propio logger.
    El logger heredará la configuración centralizada de setup_logging().

    Args:
        module_name: Nombre del módulo (típicamente __name__)

    Returns:
        logging.Logger: Logger configurado para el módulo
    """
    return logging.getLogger(module_name)

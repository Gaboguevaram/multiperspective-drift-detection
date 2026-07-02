Formato:

"[Código]-[Tamaño_log]-single.csv"

Códigos (patrones de cambio del benchmark de Maaradji):

    - cb: un fragmento pasa de obligatorio a omitible (skippable) -> cae el FITNESS, la precisión se mantiene.

    - pl: dos fragmentos concurrentes pasan a ejecutarse en secuencia -> cae la PRECISIÓN, el fitness se mantiene.

A diferencia de los logs originales de Maaradji (../<código>-<tamaño>.xes), que introducen un cambio cada 10% de trazas (un cambio cada 250 trazas para un log de 2500, cada 500 para uno de 5000), estos logs se han reconstruido para contener UN ÚNICO cambio sostenido al 50% del log.

La reconstrucción (src/log_generation/control_flow/reconstruir_cambio_unico.py) reordena físicamente los 10 bloques del log original juntando primero los 5 bloques del régimen base y después los 5 del régimen drift, renumerando los case ids de forma única y reescribiendo los timestamps de forma monótona. El proceso es el de solicitud de préstamos, con un evento por minuto.

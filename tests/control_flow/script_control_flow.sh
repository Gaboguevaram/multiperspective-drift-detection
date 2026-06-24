#!/bin/bash

LOGS=(

        "./data/01_raw/control_flow/recurring/cm-2500.xes"
        "./data/01_raw/control_flow/recurring/cm-5000.xes"
        "./data/01_raw/control_flow/recurring/cm-7500.xes"
        "./data/01_raw/control_flow/recurring/cm-10000.xes"

        "./data/01_raw/control_flow/recurring/cp-2500.xes"
        "./data/01_raw/control_flow/recurring/cp-5000.xes"
        "./data/01_raw/control_flow/recurring/cp-7500.xes"
        "./data/01_raw/control_flow/recurring/cp-10000.xes"

        "./data/01_raw/control_flow/recurring/pm-2500.xes"
        "./data/01_raw/control_flow/recurring/pm-5000.xes"
        "./data/01_raw/control_flow/recurring/pm-7500.xes"
        "./data/01_raw/control_flow/recurring/pm-10000.xes"
        
        "./data/01_raw/control_flow/recurring/re-2500.xes"
        "./data/01_raw/control_flow/recurring/re-5000.xes"
        "./data/01_raw/control_flow/recurring/re-7500.xes"
        "./data/01_raw/control_flow/recurring/re-10000.xes"

        "./data/01_raw/control_flow/recurring/rp-2500.xes"
        "./data/01_raw/control_flow/recurring/rp-5000.xes"
        "./data/01_raw/control_flow/recurring/rp-7500.xes"
        "./data/01_raw/control_flow/recurring/rp-10000.xes"

        "./data/01_raw/control_flow/recurring/sw-2500.xes"
        "./data/01_raw/control_flow/recurring/sw-5000.xes"
        "./data/01_raw/control_flow/recurring/sw-7500.xes"
        "./data/01_raw/control_flow/recurring/sw-10000.xes"

        "./data/01_raw/control_flow/recurring/cb-2500.xes"
        "./data/01_raw/control_flow/recurring/cb-5000.xes"
        "./data/01_raw/control_flow/recurring/cb-7500.xes"
        "./data/01_raw/control_flow/recurring/cb-10000.xes"

        "./data/01_raw/control_flow/recurring/lp-2500.xes"
        "./data/01_raw/control_flow/recurring/lp-5000.xes"
        "./data/01_raw/control_flow/recurring/lp-7500.xes"
        "./data/01_raw/control_flow/recurring/lp-10000.xes"

        "./data/01_raw/control_flow/recurring/cd-2500.xes"
        "./data/01_raw/control_flow/recurring/cd-5000.xes"
        "./data/01_raw/control_flow/recurring/cd-7500.xes"
        "./data/01_raw/control_flow/recurring/cd-10000.xes"

        "./data/01_raw/control_flow/recurring/cf-2500.xes"
        "./data/01_raw/control_flow/recurring/cf-5000.xes"
        "./data/01_raw/control_flow/recurring/cf-7500.xes"
        "./data/01_raw/control_flow/recurring/cf-10000.xes"

        "./data/01_raw/control_flow/recurring/pl-2500.xes"
        "./data/01_raw/control_flow/recurring/pl-5000.xes"
        "./data/01_raw/control_flow/recurring/pl-7500.xes"
        "./data/01_raw/control_flow/recurring/pl-10000.xes"

        "./data/01_raw/control_flow/recurring/ior-2500.xes"
        "./data/01_raw/control_flow/recurring/ior-5000.xes"
        "./data/01_raw/control_flow/recurring/ior-7500.xes"
        "./data/01_raw/control_flow/recurring/ior-10000.xes"

        "./data/01_raw/control_flow/recurring/iro-2500.xes"
        "./data/01_raw/control_flow/recurring/iro-5000.xes"
        "./data/01_raw/control_flow/recurring/iro-7500.xes"
        "./data/01_raw/control_flow/recurring/iro-10000.xes"

        "./data/01_raw/control_flow/recurring/oir-2500.xes"
        "./data/01_raw/control_flow/recurring/oir-5000.xes"
        "./data/01_raw/control_flow/recurring/oir-7500.xes"
        "./data/01_raw/control_flow/recurring/oir-10000.xes"

        "./data/01_raw/control_flow/recurring/ori-2500.xes"
        "./data/01_raw/control_flow/recurring/ori-5000.xes"
        "./data/01_raw/control_flow/recurring/ori-7500.xes"
        "./data/01_raw/control_flow/recurring/ori-10000.xes"

        "./data/01_raw/control_flow/recurring/roi-2500.xes"
        "./data/01_raw/control_flow/recurring/roi-5000.xes"
        "./data/01_raw/control_flow/recurring/roi-7500.xes"
        "./data/01_raw/control_flow/recurring/roi-10000.xes"

        "./data/01_raw/control_flow/recurring/rio-2500.xes"
        "./data/01_raw/control_flow/recurring/rio-5000.xes"
        "./data/01_raw/control_flow/recurring/rio-7500.xes"
        "./data/01_raw/control_flow/recurring/rio-10000.xes"
    )

# Número máximo de ejecuciones simultáneas. Lanzar las 11 a la vez satura el equipo,
# así que se limita el número de procesos en paralelo.
# Por defecto: la mitad de los núcleos disponibles (mínimo 1), un valor conservador.
# Se puede sobrescribir al lanzar: MAX_JOBS=4 bash tests/control_flow/script_control_flow.sh
if [[ -z "$MAX_JOBS" ]]; then
    NUCLEOS=$(nproc 2>/dev/null || echo "${NUMBER_OF_PROCESSORS:-2}")
    MAX_JOBS=$(( NUCLEOS / 2 ))
    [[ "$MAX_JOBS" -lt 1 ]] && MAX_JOBS=1
fi

# Carpeta donde se guarda la salida de cada ejecución. Con varios procesos en paralelo
# la salida por terminal se entremezcla y es ilegible, así que cada log va a su propio fichero.
DIR_SALIDA="tests/control_flow/salida_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$DIR_SALIDA"

echo "Lanzando ${#LOGS[@]} ejecuciones con un máximo de $MAX_JOBS en paralelo."
echo "Salida de cada ejecución en: $DIR_SALIDA"

# Se debe lanzar el script desde la raíz del proyecto para que funcione correctamente
for LOG in "${LOGS[@]}"; do
    # Si ya hay MAX_JOBS procesos en ejecución, esperar a que termine alguno
    # antes de lanzar el siguiente (control de concurrencia).
    while (( $(jobs -rp | wc -l) >= MAX_JOBS )); do
        wait -n
    done

    NOMBRE=$(basename "$LOG" .xes)
    echo "Lanzando: $LOG"
    python -m tests.test_perspectivas --log "$LOG" --perspectiva 'control_flow' -f 'conf/estado_arte/cf.yml' \
        > "$DIR_SALIDA/$NOMBRE.log" 2>&1 &
done

echo "Todos los comandos lanzados. Esperando a que terminen los pendientes..."
wait
echo "Todos los comandos completados. Salida en: $DIR_SALIDA"

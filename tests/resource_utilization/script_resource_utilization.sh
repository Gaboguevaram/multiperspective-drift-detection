#!/bin/bash

LOGS=(
    "./data/01_raw/resource_utilization/Caso_B/TP_sudden_caso_b_2500.csv"
    "./data/01_raw/resource_utilization/Caso_B/TP_sudden_caso_b_5000.csv"
    "./data/01_raw/resource_utilization/Caso_B/TP_sudden_caso_b_7500.csv"
    "./data/01_raw/resource_utilization/Caso_B/TP_sudden_caso_b_10000.csv"
)

# Se debe lanzar el script desde la raíz del proyecto para que funcione correctamente
for LOG in "${LOGS[@]}"; do
    echo "Lanzando: $LOG"
    bash -c "cd $(pwd) && python -m tests.test_perspectivas --log '$LOG' --perspectiva 'resource_utilization' --file './conf/resource_utilization.yml'" 
done

wait
echo "Todos los comandos completados."

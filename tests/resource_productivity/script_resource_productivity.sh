#!/bin/bash

LOGS=(
    "./data/01_raw/resource_productivity/Caso_E/TP_sudden_caso_e_2500.csv"
    "./data/01_raw/resource_productivity/Caso_E/TP_sudden_caso_e_5000.csv"
    "./data/01_raw/resource_productivity/Caso_E/TP_sudden_caso_e_7500.csv"
)

# Se debe lanzar el script desde la raíz del proyecto para que funcione correctamente
for LOG in "${LOGS[@]}"; do
    echo "Lanzando: $LOG"
    bash -c "cd $(pwd) && python -m tests.test_perspectivas --log '$LOG' --perspectiva 'resource_productivity' --file './conf/resource_productivity.yml'" 
done

wait
echo "Todos los comandos completados."

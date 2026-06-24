#!/bin/bash

LOGS=(
    "./data/01_raw/arrival_rate/N-300-30-7200-600_2500.csv"
    "./data/01_raw/arrival_rate/N-300-30-7200-600_5000.csv"
    "./data/01_raw/arrival_rate/N-300-30-7200-600_7500.csv"
    "./data/01_raw/arrival_rate/N-300-30-7200-600_10000.csv"

    "./data/01_raw/arrival_rate/N-900-90-14400-1200_2500.csv"
    "./data/01_raw/arrival_rate/N-900-90-14400-1200_5000.csv"
    "./data/01_raw/arrival_rate/N-900-90-14400-1200_7500.csv"
    "./data/01_raw/arrival_rate/N-900-90-14400-1200_10000.csv"

    "./data/01_raw/arrival_rate/N-1800-150-600-60_2500.csv"
    "./data/01_raw/arrival_rate/N-1800-150-600-60_5000.csv"
    "./data/01_raw/arrival_rate/N-1800-150-600-60_7500.csv"
    "./data/01_raw/arrival_rate/N-1800-150-600-60_10000.csv"

    "./data/01_raw/arrival_rate/N-3600-300-600-60_2500.csv"
    "./data/01_raw/arrival_rate/N-3600-300-600-60_5000.csv"
    "./data/01_raw/arrival_rate/N-3600-300-600-60_7500.csv"
    "./data/01_raw/arrival_rate/N-3600-300-600-60_10000.csv"
)

# Se debe lanzar el script desde la raíz del proyecto para que funcione correctamente
for LOG in "${LOGS[@]}"; do
    echo "Lanzando: $LOG"
    bash -c "cd $(pwd) && python -m tests.test_perspectivas --log '$LOG' --perspectiva 'arrival_rate' --file './conf/arrival_rate.yml'"
done

echo "Todos los comandos lanzados en paralelo."
wait
echo "Todos los comandos completados."


#!/bin/bash

LOGS=(
    "./data/01_raw/calendar/Caso_A/Ac_recurring_caso_a_5000.csv"
    "./data/01_raw/calendar/Caso_A/Ac_recurring_caso_a_7500.csv"
    "./data/01_raw/calendar/Caso_A/Ac_recurring_caso_a_10000.csv"
    "./data/01_raw/calendar/Caso_A/Ac_recurring_caso_a_12500.csv"
    "./data/01_raw/calendar/Caso_A/Ac_sudden_caso_a_5000.csv"
    "./data/01_raw/calendar/Caso_A/Ac_sudden_caso_a_7500.csv"
    "./data/01_raw/calendar/Caso_A/Ac_sudden_caso_a_10000.csv"
    "./data/01_raw/calendar/Caso_A/Ac_sudden_caso_a_12500.csv"

    "./data/01_raw/calendar/Caso_B/Rc_recurring_caso_b_5000.csv"
    "./data/01_raw/calendar/Caso_B/Rc_recurring_caso_b_7500.csv"
    "./data/01_raw/calendar/Caso_B/Rc_recurring_caso_b_10000.csv"
    "./data/01_raw/calendar/Caso_B/Rc_recurring_caso_b_12500.csv"
    "./data/01_raw/calendar/Caso_B/Rc_sudden_caso_b_5000.csv"
    "./data/01_raw/calendar/Caso_B/Rc_sudden_caso_b_7500.csv"
    "./data/01_raw/calendar/Caso_B/Rc_sudden_caso_b_10000.csv"
    "./data/01_raw/calendar/Caso_B/Rc_sudden_caso_b_12500.csv"
)

# Se debe lanzar el script desde la raíz del proyecto para que funcione correctamente
for LOG in "${LOGS[@]}"; do
    echo "Lanzando: $LOG"
    bash -c "cd $(pwd) && python -m tests.test_perspectivas --log '$LOG' --perspectiva 'calendar' --file './conf/calendar.yml'" 
done

wait
echo "Todos los comandos completados."


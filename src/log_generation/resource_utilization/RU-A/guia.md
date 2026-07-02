# Caso B — Servicio más lento (RU-A, 2 recursos)

Dos recursos con tareas exclusivas: Marta (tareas A, B, C) y Carlos (tareas D, E; control). El calendario de ambos se mantiene constante (lunes-viernes, 24 h). Lo que cambia es el tiempo de servicio de Marta: cada tarea pasa de ~200 s a ~340 s. Carlos mantiene su tiempo de servicio.

- Tipo de cambio: aumento del tiempo de servicio de Marta (T_P↑) con tiempo disponible constante.
- Métrica que reacciona: utilización de Marta (↑); la de Carlos permanece plana (control).
- Como la utilización es por recurso y las tareas son exclusivas, el cambio de Marta no afecta a Carlos.
- Corresponde a RU-A en la batería de logs simples.

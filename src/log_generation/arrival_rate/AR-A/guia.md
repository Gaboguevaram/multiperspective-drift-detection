# AR-A — Aumento abrupto de la tasa de llegada (AR-A)

Un único recurso (capacidad efectivamente infinita) y un calendario de llegada de lunes a viernes de 8 a 18 h. En el log base los casos llegan con una entrellegada normal de media 1800 s (≈30 min); tras el cambio la media baja a 900 s (≈15 min), de modo que llegan el doble de casos por unidad de tiempo.

- Tipo de cambio: aumento abrupto de la tasa de llegada (solo cambia `arrival_time_distribution`: norm(1800,1500) → norm(900,750)).
- Métrica que reacciona: la tasa de llegada (eventos de la primera tarea por ventana) sube en escalón sostenido.
- El servicio y el resto de la configuración no cambian; el cambio queda aislado en la perspectiva de arrival rate.
- Caso usado para AR-A en la batería de logs simples.

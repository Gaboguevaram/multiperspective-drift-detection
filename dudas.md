Lista TODOs post memoria:

- Implementar el warm up para los recuross y calendarios, prohibir el calculo de metricas en ese paseo

- Limpiar codigo

- Comentar bn todas las funciones

- Actualizar README

--------------------------

- Todas las metricas tienen direcciones

- Si un recurso aparece a medias, tener una lista de recurso, sse detcta un cambio de calendario:

  - Que significa esto: Las perspectivas asociadas a un elemento externo deben tener un listado de esos elementos en su modelo inicial. En otras palabras para los calendarios tenemos entonces un listado de pares recurso-tarea. El paso a seguir es muy sencillo. Al descubrir al modelo Se generan esas dos listas la de recursos o la de pares dependiendo de la perspectiva. Si en algún momento Aparece un nuevo recurso por ejemplo para los calendarios Se detecta un cambio De tal manera que se redescubre el modelo pero únicamente se redescubre el modelo para ese elemento nuevo O sea solo sé descubrir el calendario de ese recurso Pero se manda igualmente una señal de Detección de cambio ¿Por qué? Para poder pasárselo a las perspectivas dependientes de esta.
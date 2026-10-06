# ETL automatico por cambios en el CSV

1. Inicia la demo con `docker compose up -d --build`.
2. Abre [Dagster](http://localhost:3000) y busca `ventas_csv_sensor` en **Automation / Sensors**. Su estado predeterminado es activo; puedes pausarlo desde la interfaz.
3. Edita y guarda `data/ventas.csv`, conservando las columnas y el formato.
4. El sensor revisa `SOURCE_PATH` aproximadamente cada 30 segundos. Espera dos lecturas con el mismo SHA-256 antes de solicitar una ejecucion: el primer archivo tambien se procesa automaticamente.
5. Revisa la ejecucion en **Runs**, los eventos en [Seq](http://localhost:5341) y los resultados en [SQLPad](http://localhost:3001).

El sensor espera mientras haya una ejecucion activa de `etl_ventas` o una materializacion manual de assets (`__ASSET_JOB`) en Dagster. Conserva su estado en el volumen `dagster_data`: reiniciar los contenedores no repite el ultimo contenido enviado. Las etiquetas `source_sha256`, `source_path` y `etl_trigger` permiten identificar el origen de cada ejecucion. Bronze conserva los originales por hash; Silver, Gold y PostgreSQL representan el contenido completo del CSV actual.

Tocar o guardar el archivo sin cambiar sus bytes no dispara otra ejecucion. Volver a una version anterior despues de un cambio si crea una nueva revision para actualizar Gold. El asset Bronze comprueba que el hash detectado coincide con los bytes que va a procesar.

Si falla una ejecucion, el sensor no insiste indefinidamente con el mismo contenido. Corrige el CSV y guardalo con cambios, o reejecuta desde Dagster cuando hayas resuelto el error de infraestructura. Si el archivo cambia antes de iniciar Bronze, espera el nuevo procesamiento automatico.

Esta automatizacion procesa el CSV configurado como una carga completa. La carga incremental y la ingesta de multiples archivos son mejoras independientes. El sensor y sus motivos de espera se consultan en Dagster; las etapas del ETL y sus excepciones se envian a Seq.

Referencias: [sensores y run keys](https://dagster.io/docs/guides/automate/sensors).

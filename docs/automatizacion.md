# Automatización del ETL

Dagster revisa las fuentes aproximadamente cada 30 segundos.

| Fuente | Sensor | Comportamiento |
|---|---|---|
| CSV | `ventas_csv_sensor` | Espera dos lecturas iguales y verifica la huella antes de capturar. |
| PostgreSQL | `ventas_db_sensor` | Detecta un cambio con una lectura; el job captura su propio snapshot consistente. |

## Uso

1. Edita `data/ventas.csv` o agrega una venta con [operacion.sql](../sql/operacion.sql).
2. Revisa **Runs** en [Dagster](http://localhost:3000).
3. Consulta Gold en [SQLPad](http://localhost:3001) y los eventos en [Seq](http://localhost:5341).

Los sensores se administran desde **Automation / Sensors**. Su estado se conserva al reiniciar. El mismo contenido no genera otra solicitud automática; ante fallos, corrige la fuente o reintenta desde Dagster.

Cada ejecución conserva sus propios resultados en MinIO. PostgreSQL registra la versión vigente junto con Gold y calidad. Una fuente válida vacía publica cero registros; una inválida conserva la última publicación.

[Arquitectura y versiones](arquitectura.md).

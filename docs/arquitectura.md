# Arquitectura limpia

- **domain:** datos y reglas de ventas, sin servicios externos.
- **etl:** casos de uso y contratos `Protocol`.
- **infra:** CSV, PostgreSQL, MinIO y Parquet.
- **jobs:** Dagster y consola.
- **bootstrap:** configuración y construcción de dependencias.

Una prueba automática impide que `domain` y `etl` importen infraestructura o frameworks.

## Versiones

Bronze conserva los bytes por hash. Cada ejecución guarda sus propios archivos:

- `silver/<dataset>/runs/<execution_id>/`: ventas limpias, rechazados y calidad.
- `gold/<dataset>/runs/<execution_id>/`: ventas agregadas y manifiesto.

El manifiesto contiene referencias, hashes y conteos. Su existencia indica que los archivos están completos; la versión publicada se consulta en `etl_control.publicaciones`, dentro del PostgreSQL Gold.

Gold, calidad y esa referencia se confirman en una sola transacción. Una revisión anterior no reemplaza una posterior, y reintentar una publicación confirmada no la duplica.

## Fallos y datos vacíos

- Fuente válida vacía: publica cero registros.
- Fuente ilegible o totalmente inválida: conserva la publicación anterior y reporta error.
- Escritura parcial: sus archivos quedan sin publicar; se puede reintentar desde Dagster.
- Reintento parcial: utiliza las referencias del snapshot original. Para capturar datos nuevos, ejecuta el job completo.

Las rutas fijas anteriores se conservan como archivos antiguos y dejan de actualizarse. Las tablas de SQLPad mantienen sus nombres. No hay eliminación automática del historial.

Consulta las versiones con [consultas.sql](../sql/consultas.sql).
